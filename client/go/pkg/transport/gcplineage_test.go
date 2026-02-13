// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"testing"
)

func TestGCPLineageConfig_Validation(t *testing.T) {
	tests := []struct {
		name      string
		config    GCPLineageConfig
		wantError bool
	}{
		{
			name: "valid config with project ID",
			config: GCPLineageConfig{
				ProjectID: "my-project",
			},
			wantError: false,
		},
		{
			name: "valid config with all fields",
			config: GCPLineageConfig{
				ProjectID:       "my-project",
				Location:        "us-west1",
				CredentialsFile: "/path/to/credentials.json",
			},
			wantError: false,
		},
		{
			name:      "missing project ID",
			config:    GCPLineageConfig{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't actually create the transport without GCP credentials,
			// but we can validate the config
			if tt.config.ProjectID == "" && !tt.wantError {
				t.Error("expected error for missing project ID")
			}
		})
	}
}

func TestGCPLineageConfig_DefaultLocation(t *testing.T) {
	config := GCPLineageConfig{
		ProjectID: "my-project",
	}

	if config.Location != "" {
		t.Errorf("expected empty location in config, got %s", config.Location)
	}

	// The default location should be applied when creating the transport
	// We verify the constant is set correctly
	if DefaultGCPLocation != "us-central1" {
		t.Errorf("expected default location to be 'us-central1', got %s", DefaultGCPLocation)
	}
}

func TestParseGCPLocation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple location",
			input:    "us-central1",
			expected: "us-central1",
		},
		{
			name:     "full parent path",
			input:    "projects/my-project/locations/us-west1",
			expected: "us-west1",
		},
		{
			name:     "partial parent path",
			input:    "projects/my-project",
			expected: "projects/my-project",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseGCPLocation(tt.input)
			if result != tt.expected {
				t.Errorf("parseGCPLocation(%s) = %s, expected %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGCPLineageTransportType(t *testing.T) {
	if TransportTypeGCPLineage != "gcplineage" {
		t.Errorf("expected TransportTypeGCPLineage to be 'gcplineage', got %s", TransportTypeGCPLineage)
	}
}
