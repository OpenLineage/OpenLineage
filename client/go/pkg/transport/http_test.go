/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package transport

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// capturedRequest holds the raw body and headers of an HTTP request.
type capturedRequest struct {
	Body    []byte
	Headers http.Header
}

// testServer creates an httptest.Server that collects all POST requests.
func testServer(t *testing.T, statusCode int) (*httptest.Server, *[]capturedRequest) {
	t.Helper()

	var mu sync.Mutex
	var reqs []capturedRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		mu.Lock()
		reqs = append(reqs, capturedRequest{Body: body, Headers: r.Header.Clone()})
		mu.Unlock()
		w.WriteHeader(statusCode)
	}))

	t.Cleanup(srv.Close)

	return srv, &reqs
}

// newHTTPTransport is a helper that creates an httpTransport via the public New() factory.
func newHTTPTransport(t *testing.T, cfg HTTPConfig) Transport {
	t.Helper()
	tr, err := New(&Config{Type: TransportTypeHTTP, HTTP: cfg})
	if err != nil {
		t.Fatalf("New(HTTP): %v", err)
	}
	return tr
}

// TestHTTPTransport_Emit_BasicPost verifies that Emit sends exactly one POST
// request with Content-Type application/json and the payload serialised as JSON.
func TestHTTPTransport_Emit_BasicPost(t *testing.T) {
	t.Parallel()

	srv, reqs := testServer(t, http.StatusOK)

	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL})
	payload := map[string]string{"eventType": "START"}

	_, err := tr.Emit(context.Background(), payload)
	if err != nil {
		t.Fatalf("Emit() error: %v", err)
	}

	if len(*reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(*reqs))
	}

	req := (*reqs)[0]
	if ct := req.Headers.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var decoded map[string]string
	if err := json.Unmarshal(req.Body, &decoded); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if decoded["eventType"] != "START" {
		t.Errorf("body eventType = %q, want START", decoded["eventType"])
	}
}

// TestHTTPTransport_Emit_CreatedStatusAccepted verifies that a 201 Created
// response is treated as success and does not cause Emit to return an error.
func TestHTTPTransport_Emit_CreatedStatusAccepted(t *testing.T) {
	t.Parallel()

	srv, _ := testServer(t, http.StatusCreated)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL})

	_, err := tr.Emit(context.Background(), map[string]string{"k": "v"})
	if err != nil {
		t.Errorf("Emit() with 201 Created should not return error, got: %v", err)
	}
}

// TestHTTPTransport_Emit_ServerError verifies that a 5xx response from the server
// is surfaced as an error from Emit.
func TestHTTPTransport_Emit_ServerError(t *testing.T) {
	t.Parallel()

	srv, _ := testServer(t, http.StatusInternalServerError)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL})

	_, err := tr.Emit(context.Background(), map[string]string{"k": "v"})
	if err == nil {
		t.Error("Emit() with 500 response should return error, got nil")
	}
}

// TestHTTPTransport_Emit_GzipCompression verifies that when CompressionGzip is
// configured the request body is gzip-compressed and the Content-Encoding header
// is set to "gzip", with the payload still decodable after decompression.
func TestHTTPTransport_Emit_GzipCompression(t *testing.T) {
	t.Parallel()

	srv, reqs := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL, Compression: CompressionGzip})

	payload := map[string]string{"eventType": "COMPLETE"}
	if _, err := tr.Emit(context.Background(), payload); err != nil {
		t.Fatalf("Emit(): %v", err)
	}

	if len(*reqs) != 1 {
		t.Fatalf("expected 1 request, got %d", len(*reqs))
	}
	req := (*reqs)[0]

	if enc := req.Headers.Get("Content-Encoding"); enc != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", enc)
	}

	gr, err := gzip.NewReader(io.NopCloser(bytes.NewReader(req.Body)))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	decompressed, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read gzip body: %v", err)
	}

	var decoded map[string]string
	if err := json.Unmarshal(decompressed, &decoded); err != nil {
		t.Fatalf("unmarshal decompressed body: %v", err)
	}
	if decoded["eventType"] != "COMPLETE" {
		t.Errorf("decompressed eventType = %q, want COMPLETE", decoded["eventType"])
	}
}

// TestHTTPTransport_Emit_APIKeyAuth verifies that an apiKey auth config causes
// an "Authorization: Bearer <key>" header to be added to every request.
func TestHTTPTransport_Emit_APIKeyAuth(t *testing.T) {
	t.Parallel()

	srv, reqs := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{
		URL:  srv.URL,
		Auth: &HTTPAuthConfig{Type: "apiKey", APIKey: "secret-key"},
	})

	if _, err := tr.Emit(context.Background(), map[string]string{}); err != nil {
		t.Fatalf("Emit(): %v", err)
	}

	got := (*reqs)[0].Headers.Get("Authorization")
	want := "Bearer secret-key"
	if got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

// TestHTTPTransport_Emit_JWTAuth verifies that a jwt auth config causes an
// "Authorization: Bearer <token>" header to be added to every request.
func TestHTTPTransport_Emit_JWTAuth(t *testing.T) {
	t.Parallel()

	srv, reqs := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{
		URL:  srv.URL,
		Auth: &HTTPAuthConfig{Type: "jwt", Token: "jwt-token"},
	})

	if _, err := tr.Emit(context.Background(), map[string]string{}); err != nil {
		t.Fatalf("Emit(): %v", err)
	}

	got := (*reqs)[0].Headers.Get("Authorization")
	want := "Bearer jwt-token"
	if got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

// TestHTTPTransport_Emit_CustomHeaders verifies that headers supplied in
// HTTPConfig.Headers are forwarded on every request.
func TestHTTPTransport_Emit_CustomHeaders(t *testing.T) {
	t.Parallel()

	srv, reqs := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{
		URL:     srv.URL,
		Headers: map[string]string{"X-Custom": "header-value"},
	})

	if _, err := tr.Emit(context.Background(), map[string]string{}); err != nil {
		t.Fatalf("Emit(): %v", err)
	}

	got := (*reqs)[0].Headers.Get("X-Custom")
	if got != "header-value" {
		t.Errorf("X-Custom = %q, want header-value", got)
	}
}

// TestHTTPTransport_Emit_URLParams verifies that key-value pairs in
// HTTPConfig.URLParams are appended as query parameters to the request URL.
func TestHTTPTransport_Emit_URLParams(t *testing.T) {
	t.Parallel()

	var receivedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tr := newHTTPTransport(t, HTTPConfig{
		URL:       srv.URL,
		URLParams: map[string]string{"source": "test"},
	})

	if _, err := tr.Emit(context.Background(), map[string]string{}); err != nil {
		t.Fatalf("Emit(): %v", err)
	}

	if !strings.Contains(receivedURL, "source=test") {
		t.Errorf("URL %q does not contain expected query param source=test", receivedURL)
	}
}

// TestHTTPTransport_Emit_MetadataFromResponseHeaders verifies that response
// headers returned by the server are surfaced in the metadata map from Emit.
func TestHTTPTransport_Emit_MetadataFromResponseHeaders(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Event-ID", "abc123")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL})
	meta, err := tr.Emit(context.Background(), map[string]string{})
	if err != nil {
		t.Fatalf("Emit(): %v", err)
	}

	if meta["X-Event-Id"] != "abc123" {
		t.Errorf("meta[X-Event-Id] = %q, want abc123", meta["X-Event-Id"])
	}
}

// TestHTTPTransport_Emit_DefaultEndpoint verifies that when no Endpoint is
// configured the transport appends the default path "api/v1/lineage" to the URL.
func TestHTTPTransport_Emit_DefaultEndpoint(t *testing.T) {
	t.Parallel()

	var receivedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL})
	if _, err := tr.Emit(context.Background(), map[string]string{}); err != nil {
		t.Fatalf("Emit(): %v", err)
	}

	if receivedPath != "/api/v1/lineage" {
		t.Errorf("default endpoint path = %q, want /api/v1/lineage", receivedPath)
	}
}

// TestHTTPTransport_Emit_CustomEndpoint verifies that a non-empty Endpoint value
// overrides the default path and is used as-is when building the request URL.
func TestHTTPTransport_Emit_CustomEndpoint(t *testing.T) {
	t.Parallel()

	var receivedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL, Endpoint: "api/v2/events"})
	if _, err := tr.Emit(context.Background(), map[string]string{}); err != nil {
		t.Fatalf("Emit(): %v", err)
	}

	if receivedPath != "/api/v2/events" {
		t.Errorf("custom endpoint path = %q, want /api/v2/events", receivedPath)
	}
}

// TestHTTPTransport_Close verifies that Close is a no-op for the HTTP transport
// (connections are managed by the underlying http.Client) and returns nil.
func TestHTTPTransport_Close(t *testing.T) {
	t.Parallel()

	tr := &httpTransport{}
	if err := tr.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

// TestHTTPTransport_ImplementsTransport is a compile-time check confirming that
// httpTransport satisfies the Transport interface.
func TestHTTPTransport_ImplementsTransport(t *testing.T) {
	t.Parallel()

	var _ Transport = (*httpTransport)(nil)
}
