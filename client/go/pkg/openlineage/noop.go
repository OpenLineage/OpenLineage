/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

//nolint:revive // package comment is in client.go
package openlineage

import (
	"context"

	"github.com/google/uuid"
)

var _ RunWrapper = (*noopRun)(nil)

type noopRun struct{}

// NewChild implements RunContext.
func (n *noopRun) NewChild(ctx context.Context, _ string) (context.Context, RunWrapper) {
	return ctx, &noopRun{}
}

// StartChild implements RunContext.
func (n *noopRun) StartChild(ctx context.Context, _ string) (context.Context, RunWrapper, error) {
	return ctx, &noopRun{}, nil
}

// Finish implements RunWrapper.
func (n *noopRun) Finish(error) {}

// Emit implements RunWrapper.
func (n *noopRun) Emit(context.Context, Emittable) (map[string]string, error) {
	return nil, nil
}

// NewEvent implements RunContext.
func (n *noopRun) NewEvent(EventType) *RunEvent {
	return &RunEvent{}
}

// JobName implements RunContext.
func (n *noopRun) JobName() string {
	return ""
}

// JobNamespace implements RunContext.
func (n *noopRun) JobNamespace() string {
	return ""
}

// Parent implements RunContext.
func (n *noopRun) Parent() RunWrapper {
	return &noopRun{}
}

// RunID implements RunContext.
func (n *noopRun) RunID() uuid.UUID {
	id, _ := uuid.FromBytes(make([]byte, 16))
	return id
}
