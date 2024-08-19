package openlineage

import (
	"time"

	"github.com/ThijsKoot/openlineage/client/go/pkg/facets"
)

type JobEvent struct {
	Job Job

	// The set of **input** datasets.
	Inputs []InputElement
	// The set of **output** datasets.
	Outputs []OutputElement

	BaseEvent
}

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

func NewNamespacedJobEvent(name, namespace string) *JobEvent {
	return &JobEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: schemaURL,
			EventTime: time.Now().Format(time.RFC3339),
		},
		Job: NewNamespacedJob(name, namespace),
	}
}

func NewJobEvent(name string) *JobEvent {
	return NewNamespacedJobEvent(name, DefaultNamespace)
}

func (j *JobEvent) WithFacets(facets ...facets.JobFacet) *JobEvent {
	for _, f := range facets {
		f.Apply(&j.Job.Facets)
	}

	return j
}

func (j *JobEvent) WithInputs(inputs ...InputElement) *JobEvent {
	j.Inputs = append(j.Inputs, inputs...)

	return j
}

func (j *JobEvent) WithOutputs(inputs ...OutputElement) *JobEvent {
	j.Outputs = append(j.Outputs, inputs...)

	return j
}

func NewNamespacedJob(name string, namespace string, jobFacets ...facets.JobFacet) Job {

	var job *facets.JobFacets
	for _, f := range jobFacets {
		f.Apply(&job)
	}

	return Job{
		Name:      name,
		Namespace: DefaultNamespace,
		Facets:    job,
	}
}

func NewJob(name string, jobFacets ...facets.JobFacet) Job {
	return NewNamespacedJob(name, DefaultNamespace, jobFacets...)
}
