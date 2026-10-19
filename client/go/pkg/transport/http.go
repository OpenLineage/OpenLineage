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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

var _ Transport = (*httpTransport)(nil)

// CompressionType specifies the compression algorithm used for HTTP request bodies.
type CompressionType string

const (
	// CompressionGzip compresses request bodies with gzip.
	CompressionGzip CompressionType = "gzip"

	// AuthTypeAPIKey sends a static API key as the bearer token.
	AuthTypeAPIKey = "apiKey"
	// AuthTypeJWT sends a static JWT as the bearer token.
	AuthTypeJWT = "jwt"
	// AuthTypeOAuth2 obtains an access token with the OAuth 2.0 client credentials
	// grant (RFC 6749, section 4.4).
	AuthTypeOAuth2 = "oauth2"

	// ClientAuthMethodBasic sends the client credentials in the Authorization header.
	ClientAuthMethodBasic = "client_secret_basic"
	// ClientAuthMethodPost sends the client credentials in the request body.
	ClientAuthMethodPost = "client_secret_post"

	// defaultTimeout is used when TimeoutInMillis is not set.
	defaultTimeout = 5000 * time.Millisecond

	// defaultTokenRefreshBuffer is how long before expiry an access token is refreshed.
	defaultTokenRefreshBuffer = 120 * time.Second
)

// HTTPAuthConfig holds authentication configuration for the HTTP transport.
type HTTPAuthConfig struct {
	// Type is the authentication method: "apiKey", "jwt" or "oauth2".
	Type string

	// APIKey is the bearer token used when Type is "apiKey".
	APIKey string

	// Token is the JWT token used when Type is "jwt".
	Token string

	// ClientID is the OAuth 2.0 client ID. Required when Type is
	// "oauth2".
	ClientID string

	// ClientSecret is the OAuth 2.0 client secret. Required when Type is
	// "oauth2".
	ClientSecret string

	// TokenEndpoint is the URL of the OAuth 2.0 token endpoint. Required when Type
	// is "oauth2".
	TokenEndpoint string

	// Scopes are the OAuth 2.0 scopes to request. Optional.
	Scopes []string

	// ClientAuthMethod is how the client credentials are sent to the token endpoint:
	// "client_secret_basic" (default) or "client_secret_post". Optional.
	ClientAuthMethod string

	// TokenRefreshBuffer is how long before expiry the access token is refreshed.
	// Optional, default: 120s.
	TokenRefreshBuffer time.Duration
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
	tokenSource *oauth2TokenSource
	headers     map[string]string
	compression CompressionType
}

// oauth2TokenSource fetches and caches an OAuth 2.0 access token, fetching a new one
// once the cached token is within TokenRefreshBuffer of expiring.
type oauth2TokenSource struct {
	config        clientcredentials.Config
	refreshBuffer time.Duration

	// lock guards the fields below. It is a channel rather than a sync.Mutex so that a
	// caller waiting on an in-flight token request still honours its own context.
	lock      chan struct{}
	token     *oauth2.Token
	refreshAt time.Time
}

// Token returns a cached access token, fetching a new one when none is cached or the
// cached one is about to expire. The token request is made with the caller's context and
// HTTP client, so that the transport's timeout, retries and cancellation apply to it.
func (s *oauth2TokenSource) Token(ctx context.Context, httpClient *http.Client) (*oauth2.Token, error) {
	select {
	case s.lock <- struct{}{}:
		defer func() { <-s.lock }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if s.token != nil && s.token.AccessToken != "" &&
		(s.token.Expiry.IsZero() || time.Now().Before(s.refreshAt)) {
		return s.token, nil
	}

	token, err := s.config.Token(context.WithValue(ctx, oauth2.HTTPClient, httpClient))
	if err != nil {
		return nil, err
	}
	s.token = token

	if !token.Expiry.IsZero() {
		// Refresh early, but never so early that every event triggers a token request.
		buffer := s.refreshBuffer
		if lifetime := time.Until(token.Expiry); buffer > lifetime/2 {
			buffer = lifetime / 2
		}
		s.refreshAt = token.Expiry.Add(-buffer)
	}

	return token, nil
}

// newClientCredentialsTokenSource builds a token source for the OAuth 2.0 client
// credentials grant.
func newClientCredentialsTokenSource(auth *HTTPAuthConfig) (*oauth2TokenSource, error) {
	if auth.ClientID == "" || auth.ClientSecret == "" || auth.TokenEndpoint == "" {
		return nil, errors.New("auth type " + AuthTypeOAuth2 +
			" requires ClientID, ClientSecret and TokenEndpoint")
	}

	var authStyle oauth2.AuthStyle
	switch auth.ClientAuthMethod {
	case "", ClientAuthMethodBasic:
		authStyle = oauth2.AuthStyleInHeader
	case ClientAuthMethodPost:
		authStyle = oauth2.AuthStyleInParams
	default:
		return nil, fmt.Errorf("unsupported ClientAuthMethod %q, want %q or %q",
			auth.ClientAuthMethod, ClientAuthMethodBasic, ClientAuthMethodPost)
	}

	refreshBuffer := auth.TokenRefreshBuffer
	if refreshBuffer <= 0 {
		refreshBuffer = defaultTokenRefreshBuffer
	}

	return &oauth2TokenSource{
		lock: make(chan struct{}, 1),
		config: clientcredentials.Config{
			ClientID:     auth.ClientID,
			ClientSecret: auth.ClientSecret,
			TokenURL:     auth.TokenEndpoint,
			Scopes:       auth.Scopes,
			AuthStyle:    authStyle,
		},
		refreshBuffer: refreshBuffer,
	}, nil
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
		case AuthTypeAPIKey:
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.auth.APIKey))
		case AuthTypeJWT:
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.auth.Token))
		case AuthTypeOAuth2:
			token, err := h.tokenSource.Token(ctx, h.httpClient)
			if err != nil {
				return nil, fmt.Errorf("obtain OAuth2 access token: %w", err)
			}
			token.SetAuthHeader(req)
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

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
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
