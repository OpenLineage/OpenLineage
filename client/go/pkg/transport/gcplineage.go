// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	lineage "cloud.google.com/go/datacatalog/lineage/apiv1"
	lineagepb "cloud.google.com/go/datacatalog/lineage/apiv1/lineagepb"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	DefaultGCPLocation string = "us-central1"
)

var _ Transport = (*gcpLineageTransport)(nil)

// GCPLineageConfig holds configuration for the GCP Data Catalog Lineage transport.
type GCPLineageConfig struct {
	// ProjectID is the GCP project ID where the lineage data will be stored (required)
	ProjectID string

	// Location is the GCP location (region) for the lineage service (default: "us-central1")
	Location string

	// CredentialsFile is the path to service account JSON credentials file (optional)
	// If not provided, the transport will use default credentials (ADC)
	CredentialsFile string
}

// gcpLineageTransport implements the Transport interface for GCP Data Catalog Lineage.
type gcpLineageTransport struct {
	client *lineage.Client
	parent string
}

// newGCPLineageTransport creates a new GCP Lineage transport.
func newGCPLineageTransport(ctx context.Context, config GCPLineageConfig) (*gcpLineageTransport, error) {
	if config.ProjectID == "" {
		return nil, fmt.Errorf("project_id is required for GCPLineageTransport")
	}

	location := config.Location
	if location == "" {
		location = DefaultGCPLocation
	}

	var opts []option.ClientOption
	if config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}

	client, err := lineage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create GCP lineage client: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", config.ProjectID, location)

	return &gcpLineageTransport{
		client: client,
		parent: parent,
	}, nil
}

// Emit sends an OpenLineage event to GCP Data Catalog Lineage.
func (g *gcpLineageTransport) Emit(ctx context.Context, event any) error {
	// Convert event to JSON then to structpb.Struct
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event to JSON: %w", err)
	}

	var eventMap map[string]interface{}
	if err := json.Unmarshal(eventJSON, &eventMap); err != nil {
		return fmt.Errorf("unmarshal event to map: %w", err)
	}

	openLineageStruct, err := structpb.NewStruct(eventMap)
	if err != nil {
		return fmt.Errorf("convert event to protobuf struct: %w", err)
	}

	req := &lineagepb.ProcessOpenLineageRunEventRequest{
		Parent:      g.parent,
		OpenLineage: openLineageStruct,
	}

	_, err = g.client.ProcessOpenLineageRunEvent(ctx, req)
	if err != nil {
		return fmt.Errorf("send lineage event to GCP: %w", err)
	}

	return nil
}

// Close closes the GCP Lineage client connection.
func (g *gcpLineageTransport) Close() error {
	if g.client != nil {
		return g.client.Close()
	}
	return nil
}

// GCPLineageOption is a functional option for configuring the GCP Lineage transport.
type GCPLineageOption func(*GCPLineageConfig)

// WithLocation sets the GCP location for the lineage service.
func WithLocation(location string) GCPLineageOption {
	return func(c *GCPLineageConfig) {
		c.Location = location
	}
}

// WithCredentialsFile sets the path to the service account credentials file.
func WithCredentialsFile(path string) GCPLineageOption {
	return func(c *GCPLineageConfig) {
		c.CredentialsFile = path
	}
}

// NewGCPLineageTransport creates a new GCP Lineage transport with the given project ID and options.
func NewGCPLineageTransport(ctx context.Context, projectID string, opts ...GCPLineageOption) (Transport, error) {
	config := GCPLineageConfig{
		ProjectID: projectID,
		Location:  DefaultGCPLocation,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return newGCPLineageTransport(ctx, config)
}

// parseGCPLocation extracts location from a parent string if needed.
func parseGCPLocation(location string) string {
	// Handle case where full parent path is provided
	if strings.HasPrefix(location, "projects/") {
		parts := strings.Split(location, "/")
		if len(parts) >= 4 {
			return parts[3]
		}
	}
	return location
}
