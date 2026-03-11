/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package transport //nolint:revive // package comment is in transport.go

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

var _ Transport = (*httpTransport)(nil)

// CompressionType specifies the compression algorithm used for HTTP request bodies.
type CompressionType string

const (
	// CompressionGzip compresses request bodies with gzip.
	CompressionGzip CompressionType = "gzip"

	// defaultTimeout is used when TimeoutInMillis is not set.
	defaultTimeout = 5000 * time.Millisecond
)

// HTTPAuthConfig holds authentication configuration for the HTTP transport.
type HTTPAuthConfig struct {
	// Type is the authentication method: "apiKey" or "jwt".
	Type string

	// APIKey is the bearer token used when Type is "apiKey".
	APIKey string

	// Token is the JWT token used when Type is "jwt".
	Token string
}

// HTTPConfig holds configuration for the HTTP transport.
type HTTPConfig struct {
	// URL is the base URL for HTTP requests. Required.
	URL string

	// Endpoint is appended to URL when building the request URI.
	// Optional, default: api/v1/lineage.
	Endpoint string

	// URLParams are query parameters appended to every request. Optional.
	URLParams map[string]string

	// TimeoutInMillis is the HTTP client timeout in milliseconds.
	// Optional, default: 5000.
	TimeoutInMillis int

	// Auth holds authentication configuration. Optional.
	// If nil, no Authorization header is sent.
	Auth *HTTPAuthConfig

	// Headers are additional HTTP headers sent with every request. Optional.
	Headers map[string]string

	// Compression specifies the request body compression algorithm.
	// Optional, allowed value: "gzip".
	Compression CompressionType
}

type httpTransport struct {
	httpClient  *http.Client
	uri         string
	urlParams   map[string]string
	auth        *HTTPAuthConfig
	headers     map[string]string
	compression CompressionType
}

// Close is a no-op for the HTTP transport; connections are managed by the http.Client.
func (h *httpTransport) Close() error {
	return nil
}

// Emit implements Transport.
func (h *httpTransport) Emit(ctx context.Context, event any) (map[string]string, error) {
	body, err := json.Marshal(&event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	var bodyReader io.Reader
	contentEncoding := ""

	if h.compression == CompressionGzip {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(body); err != nil {
			return nil, fmt.Errorf("gzip compress: %w", err)
		}
		if err := gz.Close(); err != nil {
			return nil, fmt.Errorf("gzip close: %w", err)
		}
		bodyReader = &buf
		contentEncoding = "gzip"
	} else {
		bodyReader = bytes.NewReader(body)
	}

	// Build URI with optional query params
	reqURL := h.uri
	if len(h.urlParams) > 0 {
		parsed, err := url.Parse(h.uri)
		if err != nil {
			return nil, fmt.Errorf("parse uri: %w", err)
		}
		q := parsed.Query()
		for k, v := range h.urlParams {
			q.Set(k, v)
		}
		parsed.RawQuery = q.Encode()
		reqURL = parsed.String()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}

	// Auth
	if h.auth != nil {
		switch h.auth.Type {
		case "apiKey":
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.auth.APIKey))
		case "jwt":
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.auth.Token))
		}
	}

	// Custom headers (applied last so they can override defaults if needed)
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute POST request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server responded with status %v: %s", resp.StatusCode, respBody)
	}

	meta := make(map[string]string)
	for key, vals := range resp.Header {
		if len(vals) > 0 {
			meta[key] = vals[0]
		}
	}

	return meta, nil
}
