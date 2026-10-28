/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package transport

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// TestHTTPTransport_Emit_AcceptedStatusAccepted verifies that a 202 Accepted
// response is treated as success, as consumers that queue events answer with it.
func TestHTTPTransport_Emit_AcceptedStatusAccepted(t *testing.T) {
	t.Parallel()

	srv, _ := testServer(t, http.StatusAccepted)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL})

	_, err := tr.Emit(context.Background(), map[string]string{"k": "v"})
	if err != nil {
		t.Errorf("Emit() with 202 Accepted should not return error, got: %v", err)
	}
}

// TestHTTPTransport_Emit_ClientErrorStatus verifies that a 4xx response is still
// reported as an error.
func TestHTTPTransport_Emit_ClientErrorStatus(t *testing.T) {
	t.Parallel()

	srv, _ := testServer(t, http.StatusUnauthorized)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL})

	_, err := tr.Emit(context.Background(), map[string]string{"k": "v"})
	if err == nil {
		t.Error("Emit() with 401 Unauthorized should return an error")
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

// tokenServer creates an httptest.Server that serves OAuth 2.0 access tokens and
// collects the token requests it received.
func tokenServer(t *testing.T, statusCode int) (*httptest.Server, *[]capturedRequest) {
	t.Helper()

	var mu sync.Mutex
	var reqs []capturedRequest
	issued := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		mu.Lock()
		reqs = append(reqs, capturedRequest{Body: body, Headers: r.Header.Clone()})
		issued++
		token := fmt.Sprintf("access-token-%d", issued)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		if statusCode == http.StatusOK {
			fmt.Fprintf(w, `{"access_token":%q,"token_type":"Bearer","expires_in":3600}`, token)
		}
	}))

	t.Cleanup(srv.Close)

	return srv, &reqs
}

// TestHTTPTransport_Emit_OAuth2ClientCredentialsAuth verifies that the client
// credentials grant obtains an access token and sends it as a bearer token, with the
// credentials presented to the token endpoint as HTTP basic auth by default.
func TestHTTPTransport_Emit_OAuth2ClientCredentialsAuth(t *testing.T) {
	t.Parallel()

	tokenSrv, tokenReqs := tokenServer(t, http.StatusOK)
	srv, reqs := testServer(t, http.StatusOK)

	tr := newHTTPTransport(t, HTTPConfig{
		URL: srv.URL,
		Auth: &HTTPAuthConfig{
			Type:          AuthTypeOAuth2,
			ClientID:      "my-client-id",
			ClientSecret:  "my-client-secret",
			TokenEndpoint: tokenSrv.URL,
		},
	})

	if _, err := tr.Emit(context.Background(), map[string]string{"eventType": "START"}); err != nil {
		t.Fatalf("Emit() error: %v", err)
	}

	if got, want := (*reqs)[0].Headers.Get("Authorization"), "Bearer access-token-1"; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}

	if len(*tokenReqs) != 1 {
		t.Fatalf("expected 1 token request, got %d", len(*tokenReqs))
	}
	clientID, clientSecret, ok := basicAuthOf(t, (*tokenReqs)[0])
	if !ok {
		t.Fatal("token request has no basic auth credentials")
	}
	if clientID != "my-client-id" || clientSecret != "my-client-secret" {
		t.Errorf("basic auth = %q/%q, want my-client-id/my-client-secret", clientID, clientSecret)
	}
	if grant := formValueOf(t, (*tokenReqs)[0], "grant_type"); grant != "client_credentials" {
		t.Errorf("grant_type = %q, want client_credentials", grant)
	}
}

// TestHTTPTransport_Emit_OAuth2ClientSecretPost verifies that client_secret_post
// sends the credentials in the token request body instead of the Authorization header,
// and that configured scopes are forwarded.
func TestHTTPTransport_Emit_OAuth2ClientSecretPost(t *testing.T) {
	t.Parallel()

	tokenSrv, tokenReqs := tokenServer(t, http.StatusOK)
	srv, _ := testServer(t, http.StatusOK)

	tr := newHTTPTransport(t, HTTPConfig{
		URL: srv.URL,
		Auth: &HTTPAuthConfig{
			Type:             AuthTypeOAuth2,
			ClientID:         "my-client-id",
			ClientSecret:     "my-client-secret",
			TokenEndpoint:    tokenSrv.URL,
			ClientAuthMethod: ClientAuthMethodPost,
			Scopes:           []string{"openid", "lineage"},
		},
	})

	if _, err := tr.Emit(context.Background(), map[string]string{"eventType": "START"}); err != nil {
		t.Fatalf("Emit() error: %v", err)
	}

	req := (*tokenReqs)[0]
	if _, _, ok := basicAuthOf(t, req); ok {
		t.Error("token request should not use basic auth for client_secret_post")
	}
	if got := formValueOf(t, req, "client_id"); got != "my-client-id" {
		t.Errorf("client_id = %q, want my-client-id", got)
	}
	if got := formValueOf(t, req, "client_secret"); got != "my-client-secret" {
		t.Errorf("client_secret = %q, want my-client-secret", got)
	}
	if got := formValueOf(t, req, "scope"); got != "openid lineage" {
		t.Errorf("scope = %q, want \"openid lineage\"", got)
	}
}

// TestHTTPTransport_Emit_OAuth2TokenIsReused verifies that a cached access token is
// reused across events rather than fetched for every emit.
func TestHTTPTransport_Emit_OAuth2TokenIsReused(t *testing.T) {
	t.Parallel()

	tokenSrv, tokenReqs := tokenServer(t, http.StatusOK)
	srv, reqs := testServer(t, http.StatusOK)

	tr := newHTTPTransport(t, HTTPConfig{
		URL: srv.URL,
		Auth: &HTTPAuthConfig{
			Type:          AuthTypeOAuth2,
			ClientID:      "my-client-id",
			ClientSecret:  "my-client-secret",
			TokenEndpoint: tokenSrv.URL,
		},
	})

	for range 3 {
		if _, err := tr.Emit(context.Background(), map[string]string{"eventType": "START"}); err != nil {
			t.Fatalf("Emit() error: %v", err)
		}
	}

	if len(*tokenReqs) != 1 {
		t.Errorf("token requests = %d, want 1", len(*tokenReqs))
	}
	for i, req := range *reqs {
		if got, want := req.Headers.Get("Authorization"), "Bearer access-token-1"; got != want {
			t.Errorf("request %d Authorization = %q, want %q", i, got, want)
		}
	}
}

// TestHTTPTransport_Emit_OAuth2TokenEndpointError verifies that a failing token
// endpoint surfaces as an Emit error rather than an unauthenticated request.
func TestHTTPTransport_Emit_OAuth2TokenEndpointError(t *testing.T) {
	t.Parallel()

	tokenSrv, _ := tokenServer(t, http.StatusUnauthorized)
	srv, reqs := testServer(t, http.StatusOK)

	tr := newHTTPTransport(t, HTTPConfig{
		URL: srv.URL,
		Auth: &HTTPAuthConfig{
			Type:          AuthTypeOAuth2,
			ClientID:      "my-client-id",
			ClientSecret:  "my-client-secret",
			TokenEndpoint: tokenSrv.URL,
		},
	})

	_, err := tr.Emit(context.Background(), map[string]string{"eventType": "START"})
	if err == nil {
		t.Fatal("Emit() error = nil, want an error")
	}
	if !strings.Contains(err.Error(), "obtain OAuth2 access token") {
		t.Errorf("Emit() error = %v, want it to mention obtaining the access token", err)
	}
	if len(*reqs) != 0 {
		t.Errorf("lineage requests = %d, want 0 when no token could be obtained", len(*reqs))
	}
}

// TestNew_OAuth2ClientCredentialsValidation verifies that an incomplete or invalid
// client credentials configuration is rejected when the transport is created.
func TestNew_OAuth2ClientCredentialsValidation(t *testing.T) {
	t.Parallel()

	complete := HTTPAuthConfig{
		Type:          AuthTypeOAuth2,
		ClientID:      "my-client-id",
		ClientSecret:  "my-client-secret",
		TokenEndpoint: "https://auth.example.com/token",
	}

	tests := []struct {
		name    string
		mutate  func(*HTTPAuthConfig)
		wantErr string
	}{
		{"missing client id", func(a *HTTPAuthConfig) { a.ClientID = "" }, "requires ClientID"},
		{"missing client secret", func(a *HTTPAuthConfig) { a.ClientSecret = "" }, "requires ClientID"},
		{"missing token endpoint", func(a *HTTPAuthConfig) { a.TokenEndpoint = "" }, "requires ClientID"},
		{
			"unsupported client auth method",
			func(a *HTTPAuthConfig) { a.ClientAuthMethod = "private_key_jwt" },
			"unsupported ClientAuthMethod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			auth := complete
			tt.mutate(&auth)

			_, err := New(&Config{
				Type: TransportTypeHTTP,
				HTTP: HTTPConfig{URL: "http://localhost:5000", Auth: &auth},
			})
			if err == nil {
				t.Fatal("New() error = nil, want an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("New() error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

// TestNew_OAuth2ClientCredentialsRefreshBuffer verifies that a custom refresh buffer
// is accepted and that the default is applied when it is not set.
func TestNew_OAuth2ClientCredentialsRefreshBuffer(t *testing.T) {
	t.Parallel()

	for _, buffer := range []time.Duration{0, 30 * time.Second} {
		_, err := New(&Config{
			Type: TransportTypeHTTP,
			HTTP: HTTPConfig{
				URL: "http://localhost:5000",
				Auth: &HTTPAuthConfig{
					Type:               AuthTypeOAuth2,
					ClientID:           "my-client-id",
					ClientSecret:       "my-client-secret",
					TokenEndpoint:      "https://auth.example.com/token",
					TokenRefreshBuffer: buffer,
				},
			},
		})
		if err != nil {
			t.Errorf("New() with refresh buffer %v: %v", buffer, err)
		}
	}
}

// basicAuthOf returns the basic auth credentials of a captured request.
func basicAuthOf(t *testing.T, req capturedRequest) (string, string, bool) {
	t.Helper()
	r := &http.Request{Header: req.Headers}
	return r.BasicAuth()
}

// formValueOf returns a single form value from a captured request body.
func formValueOf(t *testing.T, req capturedRequest, key string) string {
	t.Helper()
	values, err := url.ParseQuery(string(req.Body))
	if err != nil {
		t.Fatalf("parse form body: %v", err)
	}
	return values.Get(key)
}

// blockingTokenServer creates a token endpoint that never answers until the test ends.
func blockingTokenServer(t *testing.T) (*httptest.Server, <-chan struct{}) {
	t.Helper()

	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))

	t.Cleanup(func() {
		close(release)
		srv.Close()
	})

	return srv, started
}

// TestHTTPTransport_Emit_OAuth2RespectsEmitContextCancellation verifies that cancelling
// the context passed to Emit also cancels an in-flight token request, so that a stalled
// token endpoint cannot block Emit indefinitely.
func TestHTTPTransport_Emit_OAuth2RespectsEmitContextCancellation(t *testing.T) {
	t.Parallel()

	tokenSrv, started := blockingTokenServer(t)
	srv, _ := testServer(t, http.StatusOK)

	tr := newHTTPTransport(t, HTTPConfig{
		URL: srv.URL,
		Auth: &HTTPAuthConfig{
			Type:          AuthTypeOAuth2,
			ClientID:      "my-client-id",
			ClientSecret:  "my-client-secret",
			TokenEndpoint: tokenSrv.URL,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := tr.Emit(ctx, map[string]string{"eventType": "START"})
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("token request did not start")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Emit() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Emit() did not return after its context was cancelled")
	}
}

// TestOAuth2TokenSource_UsesProvidedHTTPClient verifies that the token request is made
// with the HTTP client it is handed, so that the transport's timeout applies to it.
func TestOAuth2TokenSource_UsesProvidedHTTPClient(t *testing.T) {
	t.Parallel()

	tokenSrv, _ := blockingTokenServer(t)

	source, err := newClientCredentialsTokenSource(&HTTPAuthConfig{
		Type:          AuthTypeOAuth2,
		ClientID:      "my-client-id",
		ClientSecret:  "my-client-secret",
		TokenEndpoint: tokenSrv.URL,
	})
	if err != nil {
		t.Fatalf("newClientCredentialsTokenSource: %v", err)
	}

	start := time.Now()
	if _, err := source.Token(context.Background(), &http.Client{Timeout: 200 * time.Millisecond}); err == nil {
		t.Fatal("Token() error = nil, want the client timeout to surface")
	}

	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("Token() took %v, want it to fail with the client timeout", elapsed)
	}
}

// TestNew_OAuth2SurvivesCancelledConstructionContext verifies that tokens are fetched
// with the Emit context rather than the one the transport was built with, so cancelling
// the latter does not break later events.
func TestNew_OAuth2SurvivesCancelledConstructionContext(t *testing.T) {
	t.Parallel()

	tokenSrv, _ := tokenServer(t, http.StatusOK)
	srv, reqs := testServer(t, http.StatusOK)

	ctx, cancel := context.WithCancel(context.Background())
	tr, err := NewWithContext(ctx, &Config{
		Type: TransportTypeHTTP,
		HTTP: HTTPConfig{
			URL: srv.URL,
			Auth: &HTTPAuthConfig{
				Type:          AuthTypeOAuth2,
				ClientID:      "my-client-id",
				ClientSecret:  "my-client-secret",
				TokenEndpoint: tokenSrv.URL,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewWithContext: %v", err)
	}
	cancel()

	if _, err := tr.Emit(context.Background(), map[string]string{"eventType": "START"}); err != nil {
		t.Fatalf("Emit() after the construction context was cancelled: %v", err)
	}

	if got, want := (*reqs)[0].Headers.Get("Authorization"), "Bearer access-token-1"; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

// TestHTTPTransport_Emit_OAuth2ConcurrentCallerHonoursOwnContext verifies that an event
// waiting on a token request started by another event still honours its own context.
func TestHTTPTransport_Emit_OAuth2ConcurrentCallerHonoursOwnContext(t *testing.T) {
	t.Parallel()

	tokenSrv, started := blockingTokenServer(t)
	srv, _ := testServer(t, http.StatusOK)

	tr := newHTTPTransport(t, HTTPConfig{
		URL:             srv.URL,
		TimeoutInMillis: 120000,
		Auth: &HTTPAuthConfig{
			Type:          AuthTypeOAuth2,
			ClientID:      "my-client-id",
			ClientSecret:  "my-client-secret",
			TokenEndpoint: tokenSrv.URL,
		},
	})

	// Hold the first request until cleanup, and wait for it to actually start.
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = tr.Emit(firstCtx, map[string]string{"eventType": "START"})
	}()
	t.Cleanup(func() {
		cancelFirst()
		select {
		case <-firstDone:
		case <-time.After(5 * time.Second):
			t.Error("first Emit did not stop after cancellation")
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first token request did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := tr.Emit(ctx, map[string]string{"eventType": "START"})
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Emit() error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Emit() blocked past its own deadline waiting for another event's token request")
	}
}

// TestHTTPTransport_Emit_OAuth2RefreshBufferCappedAtTokenLifetime verifies that a refresh
// buffer at least as long as the token lifetime does not cause a token request per event.
func TestHTTPTransport_Emit_OAuth2RefreshBufferCappedAtTokenLifetime(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	tokenRequests := 0
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		tokenRequests++
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"access-token-value","token_type":"Bearer","expires_in":60}`)
	}))
	t.Cleanup(tokenSrv.Close)

	srv, _ := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{
		URL: srv.URL,
		Auth: &HTTPAuthConfig{
			Type:               AuthTypeOAuth2,
			ClientID:           "my-client-id",
			ClientSecret:       "my-client-secret",
			TokenEndpoint:      tokenSrv.URL,
			TokenRefreshBuffer: 120 * time.Second,
		},
	})

	for range 3 {
		if _, err := tr.Emit(context.Background(), map[string]string{"eventType": "START"}); err != nil {
			t.Fatalf("Emit() error: %v", err)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if tokenRequests != 1 {
		t.Errorf("token requests = %d, want 1", tokenRequests)
	}
}

// TestHTTPTransport_Emit_OAuth2Refresh verifies that reaching the refresh boundary
// replaces the cached token and that subsequent events reuse the replacement.
func TestHTTPTransport_Emit_OAuth2Refresh(t *testing.T) {
	t.Parallel()
	tokenSrv, tokenReqs := tokenServer(t, http.StatusOK)
	srv, reqs := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL, Auth: &HTTPAuthConfig{
		Type: AuthTypeOAuth2, ClientID: "test-client", ClientSecret: "test-secret", TokenEndpoint: tokenSrv.URL,
	}}).(*httpTransport)
	if _, err := tr.Emit(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
	// Move the refresh boundary rather than sleeping until the token expires.
	tr.tokenSource.refreshAt = time.Now().Add(-time.Second)
	for range 2 {
		if _, err := tr.Emit(context.Background(), "next"); err != nil {
			t.Fatal(err)
		}
	}
	if len(*tokenReqs) != 2 {
		t.Fatalf("token requests = %d, want 2", len(*tokenReqs))
	}
	for i, want := range []string{"Bearer access-token-1", "Bearer access-token-2", "Bearer access-token-2"} {
		if got := (*reqs)[i].Headers.Get("Authorization"); got != want {
			t.Errorf("request %d authorization = %q, want %q", i, got, want)
		}
	}
}

// testJWT returns an unsigned JWT whose payload is the given claims.
func testJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	enc := base64.RawURLEncoding
	return enc.EncodeToString([]byte(`{"alg":"none"}`)) + "." + enc.EncodeToString(payload) + ".sig"
}

// TestHTTPTransport_Emit_OAuth2TokenResponses checks cache and failure behavior for
// token endpoint responses without allowing an unauthenticated lineage request. Two
// events are sent, so wantTokens is 1 when the token is cached and 2 when it is not.
func TestHTTPTransport_Emit_OAuth2TokenResponses(t *testing.T) {
	t.Parallel()
	tokenBody := func(token string) string {
		return fmt.Sprintf(`{"access_token":%q,"token_type":"Bearer"}`, token)
	}
	now := time.Now().Unix()
	for _, tt := range []struct {
		name, body string
		wantError  bool
		wantTokens int32
	}{
		{"no expiry", tokenBody("test-token"), false, 2},
		{"jwt exp claim", tokenBody(testJWT(t, map[string]any{"exp": now + 3600})), false, 1},
		{"jwt without exp", tokenBody(testJWT(t, map[string]any{"sub": "client"})), false, 2},
		{"expired jwt", tokenBody(testJWT(t, map[string]any{"exp": now - 60})), false, 2},
		{"missing token", `{"token_type":"Bearer","expires_in":3600}`, true, 2},
		{"malformed JSON", `{"access_token":`, true, 2},
		{"non JSON", `<html>unavailable</html>`, true, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var tokenRequests atomic.Int32
			tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				tokenRequests.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tt.body)
			}))
			t.Cleanup(tokenSrv.Close)
			srv, reqs := testServer(t, http.StatusOK)
			tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL, Auth: &HTTPAuthConfig{
				Type: AuthTypeOAuth2, ClientID: "test-client", ClientSecret: "test-secret", TokenEndpoint: tokenSrv.URL,
			}})
			for range 2 {
				_, err := tr.Emit(context.Background(), "event")
				if (err != nil) != tt.wantError {
					t.Fatalf("Emit error = %v, want error %v", err, tt.wantError)
				}
			}
			wantEvents := 2
			if tt.wantError {
				wantEvents = 0
			}
			if got := tokenRequests.Load(); got != tt.wantTokens {
				t.Errorf("token requests = %d, want %d", got, tt.wantTokens)
			}
			if len(*reqs) != wantEvents {
				t.Errorf("lineage requests = %d, want %d", len(*reqs), wantEvents)
			}
		})
	}
}

// TestHTTPTransport_Emit_OAuth2ConcurrentTokenReuse checks that simultaneous emits
// fetch only one token and all send authenticated lineage events.
func TestHTTPTransport_Emit_OAuth2ConcurrentTokenReuse(t *testing.T) {
	t.Parallel()
	tokenSrv, tokenReqs := tokenServer(t, http.StatusOK)
	srv, reqs := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL, Auth: &HTTPAuthConfig{
		Type: AuthTypeOAuth2, ClientID: "test-client", ClientSecret: "test-secret", TokenEndpoint: tokenSrv.URL,
	}})
	const callers = 12
	start := make(chan struct{})
	results := make(chan error, callers)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range callers {
		go func() { <-start; _, err := tr.Emit(ctx, "event"); results <- err }()
	}
	close(start)
	for range callers {
		if err := <-results; err != nil {
			t.Errorf("Emit: %v", err)
		}
	}
	if len(*tokenReqs) != 1 {
		t.Errorf("token requests = %d, want 1", len(*tokenReqs))
	}
	if len(*reqs) != callers {
		t.Fatalf("lineage requests = %d, want %d", len(*reqs), callers)
	}
	for _, req := range *reqs {
		if got := req.Headers.Get("Authorization"); got != "Bearer access-token-1" {
			t.Errorf("authorization = %q", got)
		}
	}
}

// TestHTTPTransport_Emit_OAuth2Timeout verifies the public transport configuration
// bounds token requests even when Emit's context has no deadline.
func TestHTTPTransport_Emit_OAuth2Timeout(t *testing.T) {
	t.Parallel()
	tokenSrv, _ := blockingTokenServer(t)
	srv, reqs := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL, TimeoutInMillis: 100, Auth: &HTTPAuthConfig{
		Type: AuthTypeOAuth2, ClientID: "test-client", ClientSecret: "test-secret", TokenEndpoint: tokenSrv.URL,
	}})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, err := tr.Emit(ctx, "event"); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Emit error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Emit did not honor TimeoutInMillis for token acquisition")
	}
	if len(*reqs) != 0 {
		t.Errorf("lineage requests = %d, want 0", len(*reqs))
	}
}

// TestHTTPTransport_Emit_OAuth2AlwaysSendsBearer verifies that the access token is sent
// with the Bearer scheme whatever token_type the token endpoint returns.
func TestHTTPTransport_Emit_OAuth2AlwaysSendsBearer(t *testing.T) {
	t.Parallel()
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"test-token","token_type":"N_A","expires_in":3600}`)
	}))
	t.Cleanup(tokenSrv.Close)
	srv, reqs := testServer(t, http.StatusOK)
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL, Auth: &HTTPAuthConfig{
		Type: AuthTypeOAuth2, ClientID: "test-client", ClientSecret: "test-secret", TokenEndpoint: tokenSrv.URL,
	}})
	if _, err := tr.Emit(context.Background(), "event"); err != nil {
		t.Fatal(err)
	}
	if got, want := (*reqs)[0].Headers.Get("Authorization"), "Bearer test-token"; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

// TestHTTPTransport_Emit_AuthFixedAtConstruction verifies that changing the auth config
// after the transport is built does not change, or break, how events are authenticated.
func TestHTTPTransport_Emit_AuthFixedAtConstruction(t *testing.T) {
	t.Parallel()
	srv, reqs := testServer(t, http.StatusOK)
	auth := &HTTPAuthConfig{Type: AuthTypeJWT, Token: "my-jwt"}
	tr := newHTTPTransport(t, HTTPConfig{URL: srv.URL, Auth: auth})

	auth.Type = AuthTypeOAuth2
	auth.ClientID, auth.ClientSecret, auth.TokenEndpoint = "id", "secret", "http://127.0.0.1:0/token"

	if _, err := tr.Emit(context.Background(), "event"); err != nil {
		t.Fatal(err)
	}
	if got, want := (*reqs)[0].Headers.Get("Authorization"), "Bearer my-jwt"; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

// TestNew_UnsupportedAuthType verifies that an unknown or wrongly cased auth type fails
// at construction instead of sending events without an Authorization header.
func TestNew_UnsupportedAuthType(t *testing.T) {
	t.Parallel()
	for _, authType := range []string{"", "OAuth2", "oauth", "apikey"} {
		_, err := New(&Config{
			Type: TransportTypeHTTP,
			HTTP: HTTPConfig{URL: "http://localhost:5000", Auth: &HTTPAuthConfig{Type: authType}},
		})
		if err == nil || !strings.Contains(err.Error(), "unsupported auth type") {
			t.Errorf("New() with auth type %q: error = %v, want unsupported auth type", authType, err)
		}
	}
}
