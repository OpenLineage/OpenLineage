/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package facets

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestLineageJobIdentityFieldsMustBePaired(t *testing.T) {
	name := "partial"
	namespace := "jobs"

	tests := []struct {
		name  string
		value interface{}
	}{
		{
			name: "entry missing name",
			value: LineageJobEntry{
				Namespace: &namespace,
				Type:      LineageJobEntryTypeJob,
			},
		},
		{
			name: "input missing namespace",
			value: LineageJobInput{
				Name: &name,
				Type: LineageJobInputTypeJob,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := json.Marshal(tt.value)
			if err == nil || !strings.Contains(err.Error(), "is required when") {
				t.Fatalf("json.Marshal() error = %v, want dependentRequired error", err)
			}
		})
	}
}

func TestLineageJobIdentityFieldsAcceptBothOrNeither(t *testing.T) {
	name := "job"
	namespace := "jobs"

	values := []interface{}{
		NewLineageJobEntry(),
		LineageJobEntry{
			Name:      &name,
			Namespace: &namespace,
			Type:      LineageJobEntryTypeJob,
		},
		NewLineageJobInput(),
		LineageJobInput{
			Name:      &name,
			Namespace: &namespace,
			Type:      LineageJobInputTypeJob,
		},
	}

	for _, value := range values {
		if _, err := json.Marshal(value); err != nil {
			t.Fatalf("json.Marshal(%T) unexpected error: %v", value, err)
		}
	}
}

func TestLineageJobIdentityFieldsAreValidatedOnUnmarshal(t *testing.T) {
	var input LineageJobInput
	err := json.Unmarshal([]byte(`{"type":"JOB","name":"partial"}`), &input)
	if err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("json.Unmarshal() error = %v, want missing namespace error", err)
	}
}
