/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage_test

import (
	"testing"

	ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
)

func TestEscapeNameSegment_EscapingEnabled(t *testing.T) {
	t.Setenv("OPENLINEAGE__NAME__ESCAPING", "true")

	cases := []struct {
		input string
		want  string
	}{
		{"plain", "plain"},
		{"mydb.example.com", `mydb\.example\.com`},
		{"one.two.three", `one\.two\.three`},
		{"no_dots_here", "no_dots_here"},
		{"leading.", `leading\.`},
		{".leading", `\.leading`},
	}

	for _, tc := range cases {
		got := ol.EscapeNameSegment(tc.input)
		if got != tc.want {
			t.Errorf("EscapeNameSegment(%q) = %q, want %q", tc.input, got, tc.want)
		}
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
