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
	"net/url"

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
type Transport interface {
	Emit(ctx context.Context, event any) (map[string]string, error)
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
	switch config.Type {
	case TransportTypeConsole:
		return &consoleTransport{
			prettyPrint: config.Console.PrettyPrint,
		}, nil
	case TransportTypeHTTP:
		httpClient := retryablehttp.NewClient().StandardClient()

		u, err := url.Parse(config.HTTP.URL)
		if err != nil {
			return nil, fmt.Errorf("parsing URL \"%s\" failed: %w", config.HTTP.URL, err)
		}

		ep := config.HTTP.Endpoint
		if ep == "" {
			ep = "api/v1/lineage"
		}

		u = u.JoinPath(ep)

		return &httpTransport{
			httpClient: httpClient,
			uri:        u.String(),
			apiKey:     config.HTTP.APIKey,
		}, nil
	case TransportTypeGCPLineage:
		return newGCPLineageTransport(ctx, config.GCPLineage)
	default:
		return nil, errors.New("no valid transport specified")
	}
}
