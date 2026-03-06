/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage

import (
	"context"
	"runtime"

	"github.com/go-stack/stack"
	"github.com/google/uuid"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
)

type runContextKeyType int

const currentRunKey runContextKeyType = iota

// RunFromContext extracts the current run from a context.
// If no run is found, a noopRun is returned.
func RunFromContext(ctx context.Context) Run {
	if ctx == nil {
		return &noopRun{}
	}
	if r, ok := ctx.Value(currentRunKey).(Run); ok {
		return r
	}

	return &noopRun{}
}

// ContextWithRun returns a copy of the Context with the Run saved.
func ContextWithRun(parent context.Context, run Run) context.Context {
	return context.WithValue(parent, currentRunKey, run)
}

// Run is an instrumentation utility that allows for more ergonomic usage of the SDK.
// It is loosely modeled after the OpenTelemetry Span/Trace APIs.
type Run interface {
	Parent() Run

	// RunID returns the ID for this Run.
	RunID() uuid.UUID

	// JobName returns the name for this Run's job.
	JobName() string

	// JobNamespace returns the namespace for this Run's job.
	JobNamespace() string

	// NewChild creates a new Run with the current Run set as its parent
	NewChild(ctx context.Context, jobName string) (context.Context, Run)

	// StartChild calls NewChild and emits a START event
	StartChild(ctx context.Context, jobName string) (context.Context, Run)

	// NewEvent creates a new Event of the provided EventType
	NewEvent(EventType) *RunEvent

	// Emit emits an event with this Run's client.
	// The returned map contains any metadata from the consumer; it may be nil.
	Emit(context.Context, Emittable) (map[string]string, error)

	// Finish emits a COMPLETE event if no error has occurred, or a FAIL event otherwise.
	// If an error is passed it is recorded via RecordError before finishing — nil is ignored.
	// This allows the common pattern:
	//
	//	defer run.Finish()
	//
	// as well as the single-call error pattern:
	//
	//	err := doWork()
	//	run.Finish(err)
	Finish(errs ...error)

	// Returns true if RecordError was called for this Run.
	HasFailed() bool

	// RecordError emits an OTHER event with an ErrorMessage facet.
	// Once this is called, the run is considered to have failed.
	RecordError(error)
}

type run struct {
	parent       Run
	runID        uuid.UUID
	jobName      string
	jobNamespace string

	hasFailed bool
	client    *Client
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

func (r *run) Parent() Run {
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
		parentFacet := facets.NewParent(
			r.client.producer,
			facets.ParentJob{
				Name:      parent.JobName(),
				Namespace: parent.JobNamespace(),
			},
			facets.ParentRun{
				RunID: parent.RunID().String(),
			},
		)

		run = run.WithRunFacets(parentFacet)
	}

	return run
}

func (r *run) NewChild(ctx context.Context, jobName string) (context.Context, Run) {
	return r.client.NewRun(ctx, jobName)
}

func (r *run) StartChild(ctx context.Context, jobName string) (context.Context, Run) {
	return r.client.StartRun(ctx, jobName)
}

// Emit uses its openlineage.Client to emit an event
func (r *run) Emit(ctx context.Context, event Emittable) (map[string]string, error) {
	return r.client.Emit(ctx, event)
}

func (r *run) RecordError(err error) {
	r.hasFailed = true

	errorMessage := err.Error()

	stacktrace := stack.Caller(1).String()
	language := runtime.Version()

	errorFacet := facets.NewErrorMessage(r.client.producer, errorMessage, language).
		WithStackTrace(stacktrace)

	errorEvent := r.NewEvent(EventTypeOther).WithRunFacets(errorFacet)

	_, _ = r.client.Emit(context.Background(), errorEvent)
}

func (r *run) Finish(errs ...error) {
	for _, err := range errs {
		if err != nil {
			r.RecordError(err)
		}
	}

	eventType := EventTypeComplete
	if r.hasFailed {
		eventType = EventTypeFail
	}

	_, _ = r.client.Emit(context.Background(), r.NewEvent(eventType))
}

func (r *run) HasFailed() bool {
	return r.hasFailed
}
