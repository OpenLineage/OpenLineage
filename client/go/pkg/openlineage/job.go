/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage

import (
	"time"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
)

// JobEvent represents an OpenLineage job event.
type JobEvent struct {
	Job Job

	// The set of **input** datasets.
	Inputs []InputElement
	// The set of **output** datasets.
	Outputs []OutputElement

	BaseEvent
}

// AsEmittable converts this JobEvent to an emittable Event.
func (e *JobEvent) AsEmittable() Event {
	return Event{
		EventTime: e.EventTime,
		Job:       &e.Job,
		Inputs:    e.Inputs,
		Outputs:   e.Outputs,
		Producer:  e.Producer,
		SchemaURL: e.SchemaURL,
	}
}

// NewJobEvent creates a new JobEvent with an explicit namespace.
func NewJobEvent(name, namespace, producer string) *JobEvent {
	return &JobEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: JobEventSchemaURL,
			EventTime: time.Now(),
		},
		Job: NewJob(name, namespace),
	}
}

// WithFacets adds job facets to this JobEvent.
func (e *JobEvent) WithFacets(fs ...facets.JobFacet) *JobEvent {
	for _, f := range fs {
		f.Apply(&e.Job.Facets)
	}

	return e
}

// WithInputs adds input datasets to this JobEvent.
func (e *JobEvent) WithInputs(inputs ...InputElement) *JobEvent {
	e.Inputs = append(e.Inputs, inputs...)

	return e
}

// WithOutputs adds output datasets to this JobEvent.
func (e *JobEvent) WithOutputs(inputs ...OutputElement) *JobEvent {
	e.Outputs = append(e.Outputs, inputs...)

	return e
}

// NewJob creates a Job with an explicit namespace and optional facets.
func NewJob(name, namespace string, jobFacets ...facets.JobFacet) Job {

	var job *facets.JobFacets
	for _, f := range jobFacets {
		f.Apply(&job)
	}

	return Job{
		Name:      name,
		Namespace: namespace,
		Facets:    job,
	}
}
