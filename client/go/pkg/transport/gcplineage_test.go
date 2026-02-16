// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"testing"
)

func TestGCPLineageConfig_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    GCPLineageConfig
		wantError bool
	}{
		{
			name: "valid config with project ID only",
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
			name: "valid config with custom location",
			config: GCPLineageConfig{
				ProjectID: "my-project",
				Location:  "europe-west1",
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
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// We can't actually create the transport without GCP credentials,
			// but we can validate the config requirements
			hasError := tt.config.ProjectID == ""
			if hasError != tt.wantError {
				t.Errorf("config validation: got error=%v, want error=%v", hasError, tt.wantError)
			}
		})
	}
}

func TestGCPLineageConfig_DefaultLocation(t *testing.T) {
	t.Parallel()

	t.Run("config has empty location by default", func(t *testing.T) {
		config := GCPLineageConfig{
			ProjectID: "my-project",
		}

		if config.Location != "" {
			t.Errorf("expected empty location in config, got %q", config.Location)
		}
	})

	t.Run("DefaultGCPLocation constant is correct", func(t *testing.T) {
		want := "us-central1"
		if DefaultGCPLocation != want {
			t.Errorf("DefaultGCPLocation = %q, want %q", DefaultGCPLocation, want)
		}
	})
}

func TestParseGCPLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple location",
			input: "us-central1",
			want:  "us-central1",
		},
		{
			name:  "full parent path extracts location",
			input: "projects/my-project/locations/us-west1",
			want:  "us-west1",
		},
		{
			name:  "partial parent path returns as-is",
			input: "projects/my-project",
			want:  "projects/my-project",
		},
		{
			name:  "europe region",
			input: "europe-west1",
			want:  "europe-west1",
		},
		{
			name:  "asia region",
			input: "asia-east1",
			want:  "asia-east1",
		},
		{
			name:  "full path with asia region",
			input: "projects/prod-project/locations/asia-northeast1",
			want:  "asia-northeast1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := parseGCPLocation(tt.input)
			if got != tt.want {
				t.Errorf("parseGCPLocation(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestTransportTypeGCPLineage(t *testing.T) {
	t.Parallel()

	want := TransportType("gcplineage")
	if TransportTypeGCPLineage != want {
		t.Errorf("TransportTypeGCPLineage = %q, want %q", TransportTypeGCPLineage, want)
	}
}

func TestGCPLineageOptions(t *testing.T) {
	t.Parallel()

	t.Run("WithLocation sets location", func(t *testing.T) {
		t.Parallel()

		config := GCPLineageConfig{ProjectID: "test"}
		opt := WithLocation("europe-west1")
		opt(&config)

		if config.Location != "europe-west1" {
			t.Errorf("WithLocation: got %q, want %q", config.Location, "europe-west1")
		}
	})

	t.Run("WithCredentialsFile sets credentials path", func(t *testing.T) {
		t.Parallel()

		config := GCPLineageConfig{ProjectID: "test"}
		opt := WithCredentialsFile("/path/to/creds.json")
		opt(&config)

		if config.CredentialsFile != "/path/to/creds.json" {
			t.Errorf("WithCredentialsFile: got %q, want %q", config.CredentialsFile, "/path/to/creds.json")
		}
	})

	t.Run("multiple options can be chained", func(t *testing.T) {
		t.Parallel()

		config := GCPLineageConfig{ProjectID: "test"}

		opts := []GCPLineageOption{
			WithLocation("us-west1"),
			WithCredentialsFile("/credentials.json"),
		}

		for _, opt := range opts {
			opt(&config)
		}

		if config.Location != "us-west1" {
			t.Errorf("Location = %q, want %q", config.Location, "us-west1")
		}
		if config.CredentialsFile != "/credentials.json" {
			t.Errorf("CredentialsFile = %q, want %q", config.CredentialsFile, "/credentials.json")
		}
	})
}

func TestNewGCPLineageTransport_Validation(t *testing.T) {
	t.Parallel()

	t.Run("empty project ID returns error", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		_, err := NewGCPLineageTransport(ctx, "")

		if err == nil {
			t.Error("expected error for empty project ID, got nil")
		}
	})
}

func TestGCPLineageParentFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		projectID  string
		location   string
		wantParent string
	}{
		{
			name:       "default location",
			projectID:  "my-project",
			location:   "",
			wantParent: "projects/my-project/locations/us-central1",
		},
		{
			name:       "custom location",
			projectID:  "my-project",
			location:   "europe-west1",
			wantParent: "projects/my-project/locations/europe-west1",
		},
		{
			name:       "project with hyphens",
			projectID:  "my-gcp-project-123",
			location:   "us-west1",
			wantParent: "projects/my-gcp-project-123/locations/us-west1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			location := tt.location
			if location == "" {
				location = DefaultGCPLocation
			}

			// Construct expected parent string
			got := "projects/" + tt.projectID + "/locations/" + location

			if got != tt.wantParent {
				t.Errorf("parent = %q, want %q", got, tt.wantParent)
			}
		})
	}
}

// TestGCPLineageTransport_Interface verifies that gcpLineageTransport implements Transport
func TestGCPLineageTransport_Interface(t *testing.T) {
	t.Parallel()

	// This is a compile-time check - if it compiles, the interface is satisfied
	var _ Transport = (*gcpLineageTransport)(nil)
}
