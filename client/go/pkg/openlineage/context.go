package openlineage

import (
	"context"
	"runtime"

	"github.com/ThijsKoot/openlineage/client/go/pkg/facets"
	"github.com/go-stack/stack"
	"github.com/google/uuid"
)

type runContextKeyType int

const currentRunKey runContextKeyType = iota

// RunFromContext extracts the current run from a context.
// If no run is found, a noopRun is returned.
func RunFromContext(ctx context.Context) Run {
	if ctx == nil {
		return &noopRun{}
		// return noopSpanInstance
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
	Emit(context.Context, Emittable) error

	// Finish will emit a COMPLETE event if no error has occurred.
	// Otherwise, it will emit a FAIL event.
	Finish()

	// Returns true if RecordError was called for this Run.
	HasFailed() bool

	// RecordError emits an OTHER event with an ErrorMessage facet.
	// Once this is called, the run is considered to have failed.
	RecordError(error)

	// RecordRunFacets emits an OTHER event with the supplied RunFacets
	RecordRunFacets(...facets.RunFacet)

	// RecordJobFacets emits an OTHER event with the supplied JobFacets
	RecordJobFacets(...facets.JobFacet)

	// RecordInputs emits an OTHER event with the supplied InputElements
	RecordInputs(...InputElement)

	// RecordOutputs emits an OTHER event with the supplied OutputElements
	RecordOutputs(...OutputElement)
}

type run struct {
	parent       Run
	runID        uuid.UUID
	jobName      string
	jobNamespace string

	hasFailed bool
	client    *Client
}

// RecordFacets implements Run.
func (r *run) RecordRunFacets(facets ...facets.RunFacet) {
	r.NewEvent(EventTypeOther).
		WithRunFacets(facets...).
		Emit()
}

// RecordFacets implements Run.
func (r *run) RecordJobFacets(facets ...facets.JobFacet) {
	r.NewEvent(EventTypeOther).
		WithJobFacets(facets...).
		Emit()
}

// RecordInputs implements Run.
func (r *run) RecordInputs(inputs ...InputElement) {
	r.NewEvent(EventTypeOther).
		WithInputs(inputs...).
		Emit()
}

// RecordOutputs implements Run.
func (r *run) RecordOutputs(outputs ...OutputElement) {
	r.NewEvent(EventTypeOther).
		WithOutputs(outputs...).
		Emit()
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
	run := NewNamespacedRunEvent(eventType, r.runID, r.jobName, r.jobNamespace)

	if r.Parent() != nil {
		parent := facets.NewParent(
			facets.Job{
				Name:      r.parent.JobName(),
				Namespace: r.parent.JobNamespace(),
			},
			facets.Run{
				RunID: r.parent.RunID().String(),
			},
		)

		run = run.WithRunFacets(parent)
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
func (r *run) Emit(ctx context.Context, event Emittable) error {
	return r.client.Emit(ctx, event)
}

func (r *run) RecordError(err error) {
	r.hasFailed = true

	errorMessage := err.Error()

	stacktrace := stack.Caller(1).String()
	language := runtime.Version()

	errorFacet := facets.NewErrorMessage(errorMessage, language).
		WithStackTrace(stacktrace)

	errorEvent := r.NewEvent(EventTypeOther).WithRunFacets(errorFacet)

	_ = r.client.Emit(context.Background(), errorEvent)
}

func (r *run) Finish() {
	eventType := EventTypeComplete
	if r.hasFailed {
		eventType = EventTypeFail
	}

	_ = r.client.Emit(context.Background(), r.NewEvent(eventType))
}

func (r *run) HasFailed() bool {
	return r.hasFailed
}
