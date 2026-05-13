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

// EventTypes is a type constraint for OpenLineage event types.
type EventTypes interface {
	RunEvent | DatasetEvent | JobEvent
}

// NewNamespacedRunEvent creates a new RunEvent with an explicit namespace.
func NewNamespacedRunEvent(
	eventType EventType,
	runID uuid.UUID,
	jobName string,
	jobNamespace string,
	producer string,
) *RunEvent {
	et := eventType
	return &RunEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: RunEventSchemaURL,
			EventTime: time.Now(),
		},
		Run: Run{
			RunID: runID.String(),
		},
		EventType: &et,
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
	parent := facets.NewParentRunFacet(
		r.Producer,
		facets.ParentRunFacetJob{
			Name:      jobName,
			Namespace: namespace,
		},
		facets.ParentRunFacetRun{
			RunID: parentID.String(),
		},
	)

	return r.WithRunFacets(parent)
}
