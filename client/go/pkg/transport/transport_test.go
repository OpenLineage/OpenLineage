/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestNew_NilConfig verifies that New returns an error when called with a nil
// config pointer rather than panicking.
func TestNew_NilConfig(t *testing.T) {
	t.Parallel()

	_, err := New(nil)
	if err == nil {
		t.Error("New(nil) should return error, got nil")
	}
}

// TestNew_ConsoleTransport verifies that New successfully creates a non-nil
// console transport when TransportTypeConsole is specified.
func TestNew_ConsoleTransport(t *testing.T) {
	t.Parallel()

	tr, err := New(&Config{Type: TransportTypeConsole})
	if err != nil {
		t.Fatalf("New(console) error: %v", err)
	}
	if tr == nil {
		t.Fatal("New(console) returned nil transport")
	}
	defer tr.Close() //nolint:errcheck
}

// TestNew_ConsoleTransport_PrettyPrint verifies that New accepts a console config
// with PrettyPrint enabled and still returns a usable transport without error.
func TestNew_ConsoleTransport_PrettyPrint(t *testing.T) {
	t.Parallel()

	tr, err := New(&Config{
		Type:    TransportTypeConsole,
		Console: ConsoleConfig{PrettyPrint: true},
	})
	if err != nil {
		t.Fatalf("New(console/prettyPrint) error: %v", err)
	}
	if tr == nil {
		t.Fatal("New(console/prettyPrint) returned nil transport")
	}
	defer tr.Close() //nolint:errcheck
}

// TestNew_HTTPTransport verifies that New successfully creates a non-nil HTTP
// transport when a valid URL is provided.
func TestNew_HTTPTransport(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tr, err := New(&Config{
		Type: TransportTypeHTTP,
		HTTP: HTTPConfig{URL: srv.URL},
	})
	if err != nil {
		t.Fatalf("New(http) error: %v", err)
	}
	if tr == nil {
		t.Fatal("New(http) returned nil transport")
	}
	defer tr.Close() //nolint:errcheck
}

// TestNew_HTTPTransport_InvalidURL verifies that New returns an error when the
// HTTP config contains a URL that cannot be parsed.
func TestNew_HTTPTransport_InvalidURL(t *testing.T) {
	t.Parallel()

	_, err := New(&Config{
		Type: TransportTypeHTTP,
		HTTP: HTTPConfig{URL: "://bad-url"},
	})
	if err == nil {
		t.Error("New(http) with invalid URL should return error")
	}
}

// TestNew_UnknownTransportType verifies that New returns an error when the
// transport type string does not match any known implementation.
func TestNew_UnknownTransportType(t *testing.T) {
	t.Parallel()

	_, err := New(&Config{Type: TransportType("unknown")})
	if err == nil {
		t.Error("New with unknown transport type should return error")
	}
}

// TestNew_EmptyTransportType verifies that New returns an error when the Config
// has a zero-value (empty string) transport type.
func TestNew_EmptyTransportType(t *testing.T) {
	t.Parallel()

	_, err := New(&Config{})
	if err == nil {
		t.Error("New with empty transport type should return error")
	}
}

// TestNewWithContext_NilConfig verifies that NewWithContext returns an error when
// called with a nil config pointer, consistent with New's nil-config behaviour.
func TestNewWithContext_NilConfig(t *testing.T) {
	t.Parallel()

	_, err := NewWithContext(context.Background(), nil)
	if err == nil {
		t.Error("NewWithContext(nil) should return error")
	}
}

// TestTransportTypeConstants verifies that the three TransportType constants
// have the expected string values used in configuration files and documentation.
func TestTransportTypeConstants(t *testing.T) {
	t.Parallel()

	if TransportTypeHTTP != "http" {
		t.Errorf("TransportTypeHTTP = %q, want http", TransportTypeHTTP)
	}
	if TransportTypeConsole != "console" {
		t.Errorf("TransportTypeConsole = %q, want console", TransportTypeConsole)
	}
	if TransportTypeGCPLineage != "gcplineage" {
		t.Errorf("TransportTypeGCPLineage = %q, want gcplineage", TransportTypeGCPLineage)
	}
}
