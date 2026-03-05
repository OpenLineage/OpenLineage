/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package openlineage provides a Go client for emitting OpenLineage events.
package openlineage

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/OpenLineage/openlineage/client/go/pkg/transport"
)

// DefaultClient is a client using the console transport with pretty printing.
var DefaultClient, _ = NewClient(
	"https://github.com/OpenLineage/OpenLineage/tree/"+Version+"/client/go",
	&ClientConfig{
		Transport: transport.Config{
			Type: transport.TransportTypeConsole,
			Console: transport.ConsoleConfig{
				PrettyPrint: true,
			},
		},
	},
)

// ClientConfig holds configuration for the OpenLineage client.
type ClientConfig struct {
	Transport transport.Config

	// Namespace for events. Defaults to "default"
	Namespace string

	// When true, OpenLineage will not emit events (default: false)
	Disabled bool
}

// NewClient creates a new OpenLineage client.
// producer is a URI identifying the producer of this metadata (e.g., "https://github.com/OpenLineage/OpenLineage/tree/1.23.0/integration/spark")
func NewClient(producer string, cfg *ClientConfig) (*Client, error) {
	if cfg.Disabled {
		return &Client{
			disabled: true,
		}, nil
	}

	t, err := transport.New(&cfg.Transport)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	namespace := cfg.Namespace
	if cfg.Namespace == "" {
		namespace = "default"
	}

	return &Client{
		transport: t,
		namespace: namespace,
		producer:  producer,
	}, nil
}

// Client is the main OpenLineage client for emitting lineage events.
type Client struct {
	disabled  bool
	transport transport.Transport
	namespace string
	producer  string
}

// Emittable is implemented by event types that can be emitted via the client.
type Emittable interface {
	AsEmittable() Event
}

// Emit sends an OpenLineage event using the client's transport.
func (olc *Client) Emit(ctx context.Context, event Emittable) error {
	if olc.disabled {
		return nil
	}

	return olc.transport.Emit(ctx, event.AsEmittable())
}

// NewRun creates a Run and sets it as the active Run in ctx.
// If ctx already contains a RunContext, it is set as the parent.
func (olc *Client) NewRun(ctx context.Context, job string) (context.Context, Run) {
	r := run{
		client:       olc,
		runID:        NewRunID(),
		jobName:      job,
		jobNamespace: olc.namespace,
	}

	parent := RunFromContext(ctx)
	if _, isNoop := parent.(*noopRun); !isNoop {
		r.parent = parent
	}

	return ContextWithRun(ctx, &r), &r
}

// StartRun calls NewRun and emits a START event.
// For details, see NewRun.
func (olc *Client) StartRun(ctx context.Context, job string) (context.Context, Run) {
	ctx, r := olc.NewRun(ctx, job)

	r.NewEvent(EventTypeStart).Emit()

	return ctx, r
}

// ExistingRun recreates a Run for a given run ID.
func (olc *Client) ExistingRun(ctx context.Context, job string, runID uuid.UUID) (context.Context, Run) {
	rctx := run{
		client:       olc,
		runID:        runID,
		jobName:      job,
		jobNamespace: olc.namespace,
	}

	return ContextWithRun(ctx, &rctx), &rctx
}

// NewRun calls DefaultClient.NewRun
func NewRun(ctx context.Context, job string) (context.Context, Run) {
	return DefaultClient.NewRun(ctx, job)
}

// StartRun calls DefaultClient.StartRun
func StartRun(ctx context.Context, job string) (context.Context, Run) {
	return DefaultClient.StartRun(ctx, job)
}

// ExistingRun calls DefaultClient.ExistingRun
func ExistingRun(ctx context.Context, job string, runID uuid.UUID) (context.Context, Run) {
	return DefaultClient.ExistingRun(ctx, job, runID)
}
