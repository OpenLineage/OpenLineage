package openlineage

import (
	"time"

	"github.com/ThijsKoot/openlineage/client/go/pkg/facets"
	"github.com/google/uuid"
)

type Eventtypes interface {
	RunEvent | DatasetEvent | JobEvent
}

type RunEvent struct {
	Run RunInfo
	Job Job

	EventType EventType

	// The set of **input** datasets.
	Inputs []InputElement

	// The set of **output** datasets.
	Outputs []OutputElement

	BaseEvent
}

func (e *RunEvent) AsEmittable() Event {
	eventType := e.EventType
	if eventType == "" {
		eventType = EventTypeOther
	}

	return Event{
		EventTime: e.EventTime,
		EventType: &eventType,
		Run:       &e.Run,
		Job:       &e.Job,
		Inputs:    e.Inputs,
		Outputs:   e.Outputs,
		Producer:  e.Producer,
		SchemaURL: e.SchemaURL,
	}
}

func NewNamespacedRunEvent(
	eventType EventType,
	runID uuid.UUID,
	jobName string,
	jobNamespace string,
) *RunEvent {
	return &RunEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: schemaURL,
			EventTime: time.Now().Format(time.RFC3339),
		},
		Run: RunInfo{
			RunID: runID.String(),
		},
		EventType: eventType,
		Job: Job{
			Name:      jobName,
			Namespace: jobNamespace,
		},
	}
}

func NewRunEvent(eventType EventType, runID uuid.UUID, jobName string) *RunEvent {
	return NewNamespacedRunEvent(eventType, runID, jobName, DefaultNamespace)
}

func (r *RunEvent) WithRunFacets(runFacets ...facets.RunFacet) *RunEvent {
	for _, rf := range runFacets {
		rf.Apply(&r.Run.Facets)
	}

	return r
}

func (r *RunEvent) WithJobFacets(jobFacets ...facets.JobFacet) *RunEvent {
	for _, rf := range jobFacets {
		rf.Apply(&r.Job.Facets)
	}

	return r
}

func (r *RunEvent) WithInputs(inputs ...InputElement) *RunEvent {
	r.Inputs = append(r.Inputs, inputs...)

	return r
}

func (r *RunEvent) WithOutputs(outputs ...OutputElement) *RunEvent {
	r.Outputs = append(r.Outputs, outputs...)

	return r
}

func (r *RunEvent) WithParent(parentID uuid.UUID, jobName, namespace string) *RunEvent {
	parent := facets.NewParent(
		facets.Job{
			Name:      jobName,
			Namespace: namespace,
		},
		facets.Run{
			RunID: parentID.String(),
		},
	)

	return r.WithRunFacets(parent)
}
