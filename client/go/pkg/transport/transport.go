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
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

const (
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

		timeout := defaultTimeout
		if config.HTTP.TimeoutInMillis > 0 {
			timeout = time.Duration(config.HTTP.TimeoutInMillis) * time.Millisecond
		}

		httpClient := retryClient.StandardClient()
		httpClient.Timeout = timeout

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
