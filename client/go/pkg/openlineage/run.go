/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

//nolint:revive // package comment is in client.go
package openlineage

import (
	"time"

	"github.com/google/uuid"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
)

// Eventtypes is a type constraint for OpenLineage event types.
type Eventtypes interface {
	RunEvent | DatasetEvent | JobEvent
}

// RunEvent represents an OpenLineage run event.
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

// AsEmittable converts this RunEvent to an emittable Event.
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

// NewNamespacedRunEvent creates a new RunEvent with an explicit namespace.
func NewNamespacedRunEvent(
	eventType EventType,
	runID uuid.UUID,
	jobName string,
	jobNamespace string,
	producer string,
) *RunEvent {
	return &RunEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: RunEventSchemaURL,
			EventTime: time.Now(),
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

// NewRunEvent creates a new RunEvent using the default namespace.
func NewRunEvent(eventType EventType, runID uuid.UUID, jobName, producer string) *RunEvent {
	return NewNamespacedRunEvent(eventType, runID, jobName, DefaultNamespace, producer)
}

// WithRunFacets adds run facets to this RunEvent.
func (r *RunEvent) WithRunFacets(runFacets ...facets.RunFacet) *RunEvent {
	for _, rf := range runFacets {
		rf.Apply(&r.Run.Facets)
	}

	return r
}

// WithJobFacets adds job facets to this RunEvent.
func (r *RunEvent) WithJobFacets(jobFacets ...facets.JobFacet) *RunEvent {
	for _, rf := range jobFacets {
		rf.Apply(&r.Job.Facets)
	}

	return r
}

// WithInputs adds input datasets to this RunEvent.
func (r *RunEvent) WithInputs(inputs ...InputElement) *RunEvent {
	r.Inputs = append(r.Inputs, inputs...)

	return r
}

// WithOutputs adds output datasets to this RunEvent.
func (r *RunEvent) WithOutputs(outputs ...OutputElement) *RunEvent {
	r.Outputs = append(r.Outputs, outputs...)

	return r
}

// WithParent adds a parent run facet to this RunEvent.
func (r *RunEvent) WithParent(parentID uuid.UUID, jobName, namespace string) *RunEvent {
	parent := facets.NewParent(
		r.Producer,
		facets.ParentJob{
			Name:      jobName,
			Namespace: namespace,
		},
		facets.ParentRun{
			RunID: parentID.String(),
		},
	)

	return r.WithRunFacets(parent)
}
