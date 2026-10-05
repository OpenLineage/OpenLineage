/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package transport provides OpenLineage event transport implementations.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

const (
	maxRedirects = 10

	// TransportTypeHTTP is the HTTP transport type.
	TransportTypeHTTP TransportType = "http"
	// TransportTypeConsole is the console transport type.
	TransportTypeConsole TransportType = "console"
	// TransportTypeGCPLineage is the GCP Data Catalog Lineage transport type.
	TransportTypeGCPLineage TransportType = "gcplineage"
)

// Transport is the interface implemented by all OpenLineage transports.
// The map returned by Emit contains any response metadata from the consumer
// (e.g. a server-assigned event ID). It may be nil if the transport has nothing to report.
// Close releases any resources held by the transport (e.g. network connections).
type Transport interface {
	Emit(ctx context.Context, event any) (map[string]string, error)
	io.Closer
}

// TransportType identifies the transport implementation to use.
type TransportType string

// Config holds configuration for creating a Transport.
type Config struct {
	Type       TransportType
	Console    ConsoleConfig
	HTTP       HTTPConfig
	GCPLineage GCPLineageConfig
}

// New creates a new Transport using a background context.
func New(config *Config) (Transport, error) {
	return NewWithContext(context.Background(), config)
}

// NewWithContext creates a new Transport using the provided context.
func NewWithContext(ctx context.Context, config *Config) (Transport, error) {
	if config == nil {
		return nil, errors.New("transport config must not be nil")
	}
	switch config.Type {
	case TransportTypeConsole:
		return &consoleTransport{
			prettyPrint: config.Console.PrettyPrint,
		}, nil
	case TransportTypeHTTP:
		retryClient := retryablehttp.NewClient()
		retryClient.Logger = nil // suppress default debug logging
		checkRedirect := func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				// retryablehttp recognizes this exact shape and does not retry the redirect loop.
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}
			switch req.Response.StatusCode {
			case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther:
				return http.ErrUseLastResponse
			}
			if !sameOrigin(via[len(via)-1].URL, req.URL) {
				return http.ErrUseLastResponse
			}
			return nil
		}
		retryClient.HTTPClient.CheckRedirect = checkRedirect

		timeout := defaultTimeout
		if config.HTTP.TimeoutInMillis > 0 {
			timeout = time.Duration(config.HTTP.TimeoutInMillis) * time.Millisecond
		}

		httpClient := retryClient.StandardClient()
		httpClient.Timeout = timeout
		httpClient.CheckRedirect = checkRedirect

		u, err := url.Parse(config.HTTP.URL)
		if err != nil {
			return nil, fmt.Errorf("parsing URL %q failed: %w", config.HTTP.URL, err)
		}

		ep := config.HTTP.Endpoint
		if ep == "" {
			ep = "api/v1/lineage"
		}
		u = u.JoinPath(ep)

		return &httpTransport{
			httpClient:  httpClient,
			uri:         u.String(),
			urlParams:   config.HTTP.URLParams,
			auth:        config.HTTP.Auth,
			headers:     config.HTTP.Headers,
			compression: config.HTTP.Compression,
		}, nil
	case TransportTypeGCPLineage:
		return newGCPLineageTransport(ctx, config.GCPLineage)
	default:
		return nil, errors.New("no valid transport specified")
	}
}

func sameOrigin(first, second *url.URL) bool {
	port := originPort(first)
	return strings.EqualFold(first.Scheme, second.Scheme) &&
		strings.EqualFold(first.Hostname(), second.Hostname()) &&
		first.Hostname() != "" && port != -1 && port == originPort(second)
}

func originPort(u *url.URL) int {
	if port := u.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil {
			return -1
		}
		return value
	}
	if strings.EqualFold(u.Scheme, "http") {
		return 80
	}
	if strings.EqualFold(u.Scheme, "https") {
		return 443
	}
	return -1
}
