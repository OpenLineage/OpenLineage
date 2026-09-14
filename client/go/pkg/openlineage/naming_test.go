/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage_test

import (
	"strings"
	"testing"

	ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
)

// splitEscaped splits a dot-separated OpenLineage name back into its constituent
// segments, honouring the escape grammar: \. is a literal dot, \\ is a literal
// backslash, and an unescaped . is a structural separator.
// Used only in round-trip tests.
func splitEscaped(name string) []string {
	var segments []string
	var current strings.Builder
	i := 0
	for i < len(name) {
		ch := name[i]
		if ch == '\\' && i+1 < len(name) {
			// escape sequence: consume backslash and emit next byte literally
			current.WriteByte(name[i+1])
			i += 2
		} else if ch == '.' {
			// structural separator
			segments = append(segments, current.String())
			current.Reset()
			i++
		} else {
			current.WriteByte(ch)
			i++
		}
	}
	segments = append(segments, current.String())
	return segments
}

func TestEscapeNameSegment_RoundTrip(t *testing.T) {
	// Round-trip test: for each input name (slice of segments), escape every
	// segment, join with '.', parse back using the same grammar, and verify
	// the recovered segments match the originals.
	//
	// This validates the encoder is unambiguous — no special-character
	// combination in a segment value can be confused with a structural dot
	// or a misinterpreted escape sequence by the consumer.
	t.Setenv("OPENLINEAGE__NAME__ESCAPING", "true")

	cases := []struct {
		desc     string
		segments []string
	}{
		// ── single-segment names (no structural dot at all) ──────────────────
		{"empty segment", []string{""}},
		{"plain", []string{"plain"}},
		{"schema with dash", []string{"my_schema-1"}},
		{"non-ASCII", []string{"tëst"}},
		{"other punctuation", []string{"a/b:c"}},
		{"segment is just a dot", []string{"."}},
		{"segment is only dots", []string{"..."}},
		{"dot-wrapped word", []string{".a."}},
		{"single backslash", []string{"\\"}},
		{"two backslashes", []string{"\\\\"}},
		{"three backslashes", []string{"\\\\\\"}},
		{"backslash before dot", []string{"\\."}},
		{"backslash+dot mid-segment", []string{"a\\.b"}},
		{"two backslashes then dot", []string{"\\\\."}},
		{"dot then backslash", []string{".\\"}},
		{"backslash dot backslash", []string{"\\.\\"}},
		{"mid-dot and trailing backslash", []string{"a.b\\c"}},
		{"leading backslash then mid-dot", []string{"\\a.b"}},
		// ── multi-segment names (structural dots present) ────────────────────
		{"oracle hostname", []string{"mydb.example.com", "mySchema", "myTable"}},
		{"dots in both segments", []string{"a.b.c", "d.e"}},
		{"every segment is a bare dot", []string{".", ".", "."}},
		{"complex + plain", []string{"\\.\\", "plain"}},
		{"backslash+dot in middle segment", []string{"foo", "bar\\.baz", "qux"}},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			// Encode: escape each segment and join with '.'
			escaped := make([]string, len(tc.segments))
			for i, s := range tc.segments {
				escaped[i] = ol.EscapeNameSegment(s)
			}
			encoded := strings.Join(escaped, ".")

			// Decode: parse back and check round-trip
			recovered := splitEscaped(encoded)
			if len(recovered) != len(tc.segments) {
				t.Fatalf("segment count mismatch: got %d, want %d (encoded: %q)",
					len(recovered), len(tc.segments), encoded)
			}
			for i := range tc.segments {
				if recovered[i] != tc.segments[i] {
					t.Errorf("segment[%d]: got %q, want %q (encoded: %q)",
						i, recovered[i], tc.segments[i], encoded)
				}
			}
		})
	}
}

func TestEscapeNameSegment_EscapingDisabledByDefault(t *testing.T) {
	t.Setenv("OPENLINEAGE__NAME__ESCAPING", "")

	input := "mydb.example.com"
	got := ol.EscapeNameSegment(input)
	if got != input {
		t.Errorf("EscapeNameSegment(%q) with escaping disabled by default = %q, want %q", input, got, input)
	}
}

func TestIsNameEscapingEnabled_DefaultFalse(t *testing.T) {
	t.Setenv("OPENLINEAGE__NAME__ESCAPING", "")

	if ol.IsNameEscapingEnabled() {
		t.Error("expected escaping to be disabled by default")
	}
}

func TestIsNameEscapingEnabled_TrueVariants(t *testing.T) {
	for _, v := range []string{"true", "TRUE", "True", " true "} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("OPENLINEAGE__NAME__ESCAPING", v)
			if !ol.IsNameEscapingEnabled() {
				t.Errorf("expected escaping to be enabled for env value %q", v)
			}
		})
	}
}

func TestIsNameEscapingEnabled_NonTrueValues(t *testing.T) {
	for _, v := range []string{"false", "FALSE", "1", "yes", "on"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("OPENLINEAGE__NAME__ESCAPING", v)
			if ol.IsNameEscapingEnabled() {
				t.Errorf("expected escaping to be disabled for env value %q", v)
			}
		})
	}
}
