package openlineage

import (
	"context"
	"fmt"

	"github.com/ThijsKoot/openlineage/client/go/pkg/transport"
	"github.com/google/uuid"
)

var DefaultClient, _ = NewClient(ClientConfig{
	Transport: transport.Config{
		Type: transport.TransportTypeConsole,
		Console: transport.ConsoleConfig{
			PrettyPrint: true,
		},
	},
})

type ClientConfig struct {
	Transport transport.Config

	// Namespace for events. Defaults to "default"
	Namespace string

	// When true, OpenLineage will not emit events (default: false)
	Disabled bool
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Disabled {
		return &Client{
			disabled: true,
		}, nil
	}

	transport, err := transport.New(cfg.Transport)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	namespace := cfg.Namespace
	if cfg.Namespace == "" {
		namespace = "default"
	}

	return &Client{
		transport: transport,
		namespace: namespace,
	}, nil
}

type Client struct {
	disabled  bool
	transport transport.Transport
	namespace string
}

type Emittable interface {
	AsEmittable() Event
}

func (olc *Client) Emit(ctx context.Context, event Emittable) error {
	if olc.disabled {
		return nil
	}

	return olc.transport.Emit(ctx, event.AsEmittable())
}

// NewRun creates a Run and sets it as the active Run in ctx.
// If ctx already contains a RunContext, it set as the parent.
func (c *Client) NewRun(ctx context.Context, job string) (context.Context, Run) {
	r := run{
		client:       c,
		runID:        uuid.New(),
		jobName:      job,
		jobNamespace: c.namespace,
	}

	parent := RunFromContext(ctx)
	if _, isNoop := parent.(*noopRun); !isNoop {
		r.parent = parent
	}

	return ContextWithRun(ctx, &r), &r
}

// StartRun calls NewRun and emits a START event.
// For details, see NewRun.
func (c *Client) StartRun(ctx context.Context, job string) (context.Context, Run) {
	ctx, r := c.NewRun(ctx, job)

	r.NewEvent(EventTypeStart).Emit()

	return ctx, r
}

// ExistingRun recreates a Run for a given run ID.
func (c *Client) ExistingRun(ctx context.Context, job string, runID uuid.UUID) (context.Context, Run) {
	rctx := run{
		client:       c,
		runID:        runID,
		jobName:      job,
		jobNamespace: c.namespace,
	}

	return ContextWithRun(ctx, &rctx), &rctx
}

// NewRun calls DefaultClient.NewRun
func NewRun(ctx context.Context, job string) (context.Context, Run) {
	return DefaultClient.NewRun(ctx, job)
}

// NewRunContext calls DefaultClient.StartRun
func StartRun(ctx context.Context, job string) (context.Context, Run) {
	return DefaultClient.StartRun(ctx, job)
}

// NewRunContext calls DefaultClient.ExistingRun
func ExistingRun(ctx context.Context, job string, runID uuid.UUID) (context.Context, Run) {
	return DefaultClient.ExistingRun(ctx, job, runID)
}
