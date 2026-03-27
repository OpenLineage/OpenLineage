/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package transport

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

// captureStdout temporarily redirects os.Stdout and returns what was written.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close write pipe: %v", err)
	}
	os.Stdout = orig

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read from pipe: %v", err)
	}

	return buf.String()
}

// TestConsoleTransport_Emit_ReturnsNilMeta verifies that Emit succeeds without
// error, produces output on stdout, and always returns nil metadata.
func TestConsoleTransport_Emit_ReturnsNilMeta(t *testing.T) {
	t.Parallel()

	ct := &consoleTransport{prettyPrint: false}

	got := captureStdout(t, func() {
		meta, err := ct.Emit(context.Background(), map[string]string{"key": "value"})
		if err != nil {
			t.Errorf("Emit() error = %v, want nil", err)
		}
		if meta != nil {
			t.Errorf("Emit() meta = %v, want nil", meta)
		}
	})

	if got == "" {
		t.Error("Emit() produced no output")
	}
}

// TestConsoleTransport_Emit_ValidJSON verifies that the output written to stdout
// is a valid JSON object containing the keys from the emitted payload.
func TestConsoleTransport_Emit_ValidJSON(t *testing.T) {
	t.Parallel()

	ct := &consoleTransport{prettyPrint: false}
	payload := map[string]any{"eventType": "START", "producer": "test"}

	output := captureStdout(t, func() {
		_, _ = ct.Emit(context.Background(), payload)
	})

	output = strings.TrimSpace(output)
	if !strings.HasPrefix(output, "{") || !strings.HasSuffix(output, "}") {
		t.Errorf("output is not a JSON object: %q", output)
	}
	if !strings.Contains(output, `"eventType"`) {
		t.Errorf("output missing eventType key: %q", output)
	}
}

// TestConsoleTransport_Emit_PrettyPrint verifies that enabling PrettyPrint
// produces longer, multi-line output compared to compact mode for the same payload.
func TestConsoleTransport_Emit_PrettyPrint(t *testing.T) {
	t.Parallel()

	compact := captureStdout(t, func() {
		ct := &consoleTransport{prettyPrint: false}
		_, _ = ct.Emit(context.Background(), map[string]string{"k": "v"})
	})

	pretty := captureStdout(t, func() {
		ct := &consoleTransport{prettyPrint: true}
		_, _ = ct.Emit(context.Background(), map[string]string{"k": "v"})
	})

	// Pretty-printed output should have more whitespace / newlines.
	if len(pretty) <= len(compact) {
		t.Errorf("pretty output (%d bytes) should be longer than compact (%d bytes)", len(pretty), len(compact))
	}
	if !strings.Contains(pretty, "\n") {
		t.Errorf("pretty output should contain newlines, got: %q", pretty)
	}
}

// TestConsoleTransport_Close verifies that Close is a no-op for the console
// transport and always returns nil.
func TestConsoleTransport_Close(t *testing.T) {
	t.Parallel()

	ct := &consoleTransport{}
	if err := ct.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

// TestConsoleTransport_ImplementsTransport is a compile-time check confirming
// that consoleTransport satisfies the Transport interface.
func TestConsoleTransport_ImplementsTransport(t *testing.T) {
	t.Parallel()

	var _ Transport = (*consoleTransport)(nil)
}
