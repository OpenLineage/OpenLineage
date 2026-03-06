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

var _ Run = (*noopRun)(nil)

type noopRun struct{}

// NewChild implements RunContext.
func (n *noopRun) NewChild(ctx context.Context, _ string) (context.Context, Run) {
	return ctx, &noopRun{}
}

// StartChild implements RunContext.
func (n *noopRun) StartChild(ctx context.Context, _ string) (context.Context, Run) {
	return ctx, &noopRun{}
}

// HasFailed implements RunContext.
func (n *noopRun) HasFailed() bool {
	return false
}

// Child implements RunContext.
func (n *noopRun) Child(ctx context.Context, _ string) (context.Context, Run) {
	return ctx, &noopRun{}
}

// Finish implements RunContext.
func (n *noopRun) Finish(...error) {}

// Emit implements RunContext.
func (n *noopRun) Emit(context.Context, Emittable) (map[string]string, error) {
	return nil, nil
}

// RecordError implements RunContext.
func (n *noopRun) RecordError(error) {}

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
func (n *noopRun) Parent() Run {
	return &noopRun{}
}

// RunID implements RunContext.
func (n *noopRun) RunID() uuid.UUID {
	id, _ := uuid.FromBytes(make([]byte, 16))
	return id
}
