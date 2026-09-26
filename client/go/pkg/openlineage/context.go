/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage

import (
	"context"
	"fmt"

	"github.com/go-stack/stack"
	"github.com/google/uuid"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
)

type runContextKeyType int

const currentRunKey runContextKeyType = iota

// RunFromContext extracts the current run from a context.
// If no run is found, a noopRun is returned.
func RunFromContext(ctx context.Context) RunWrapper {
	if ctx == nil {
		return &noopRun{}
	}
	if r, ok := ctx.Value(currentRunKey).(RunWrapper); ok {
		return r
	}

	return &noopRun{}
}

// ContextWithRun returns a copy of the Context with the RunWrapper saved.
func ContextWithRun(parent context.Context, run RunWrapper) context.Context {
	return context.WithValue(parent, currentRunKey, run)
}

// RunWrapper is an instrumentation utility that allows for more ergonomic usage of the SDK.
// It is loosely modeled after the OpenTelemetry Span/Trace APIs.
type RunWrapper interface {
	Parent() RunWrapper

	// RunID returns the ID for this RunWrapper.
	RunID() uuid.UUID

	// JobName returns the name for this RunWrapper's job.
	JobName() string

	// JobNamespace returns the namespace for this RunWrapper's job.
	JobNamespace() string

	// NewChild creates a new RunWrapper with the current RunWrapper set as its parent
	NewChild(ctx context.Context, jobName string) (context.Context, RunWrapper)

	// StartChild calls NewChild and emits a START event.
	StartChild(ctx context.Context, jobName string) (context.Context, RunWrapper, error)

	// NewEvent creates a new Event of the provided EventType
	NewEvent(EventType) *RunEvent

	// Emit emits an event with this RunWrapper's client.
	// The returned map contains any metadata from the consumer; it may be nil.
	Emit(context.Context, Emittable) (map[string]string, error)

	// Finish emits a COMPLETE event if err is nil, or a FAIL event with an ErrorMessage facet otherwise.
	// This allows the common pattern:
	//
	//	defer run.Finish(nil)
	//
	// as well as the single-call error pattern:
	//
	//	err := doWork()
	//	run.Finish(err)
	Finish(err error)
}

type run struct {
	parent       RunWrapper
	runID        uuid.UUID
	jobName      string
	jobNamespace string
	client       *Client
}

// JobName implements RunContext.
func (r *run) JobName() string {
	return r.jobName
}

// JobNamespace implements RunContext.
func (r *run) JobNamespace() string {
	return r.jobNamespace
}

// RunID implements RunContext.
func (r *run) RunID() uuid.UUID {
	return r.runID
}

func (r *run) Parent() RunWrapper {
	return r.parent
}

func (r *run) NewEvent(eventType EventType) *RunEvent {
	if r == nil {
		panic("run is nil")
	}
	if r.client == nil {
		panic("run.client is nil - run was not properly initialized")
	}

	run := NewNamespacedRunEvent(eventType, r.runID, r.jobName, r.jobNamespace, r.client.producer)

	parent := r.Parent()
	if _, isNoop := parent.(*noopRun); parent != nil && !isNoop {
		parentFacet := facets.NewParentRunFacet(
			r.client.producer,
			facets.ParentRunFacetJob{
				Name:      parent.JobName(),
				Namespace: parent.JobNamespace(),
			},
			facets.ParentRunFacetRun{
				RunID: parent.RunID().String(),
			},
		)

		run = run.WithRunFacets(parentFacet)
	}

	return run
}

func (r *run) NewChild(ctx context.Context, jobName string) (context.Context, RunWrapper) {
	child := &run{
		client:       r.client,
		runID:        NewRunID(),
		jobName:      jobName,
		jobNamespace: r.jobNamespace,
		parent:       r,
	}
	return ContextWithRun(ctx, child), child
}

func (r *run) StartChild(ctx context.Context, jobName string) (context.Context, RunWrapper, error) {
	ctx, child := r.NewChild(ctx, jobName)
	if _, err := r.client.Emit(ctx, child.NewEvent(EventTypeStart)); err != nil {
		return ctx, child, fmt.Errorf("emit START event: %w", err)
	}
	return ctx, child, nil
}

// Emit uses its openlineage.Client to emit an event
func (r *run) Emit(ctx context.Context, event Emittable) (map[string]string, error) {
	return r.client.Emit(ctx, event)
}

func (r *run) Finish(err error) {
	if err != nil {
		errorMessage := err.Error()
		stacktrace := stack.Caller(1).String()

		errorFacet := facets.NewErrorMessageRunFacet(r.client.producer, errorMessage, "go").
			WithStackTrace(stacktrace)

		_, _ = r.client.Emit(context.Background(), r.NewEvent(EventTypeFail).WithRunFacets(errorFacet))
		return
	}

	_, _ = r.client.Emit(context.Background(), r.NewEvent(EventTypeComplete))
}
