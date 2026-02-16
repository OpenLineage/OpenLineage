package openlineage

import "github.com/OpenLineage/openlineage/client/go/pkg/facets"

import "time"

type Event struct {
	// the time the event occurred at
	EventTime time.Time `json:"eventTime"`
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this RunEvent
	SchemaURL string `json:"schemaURL"`
	// the current transition of the run state. It is required to issue 1 START event and 1 of [
	// COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be
	// added to the same run. For example to send additional metadata after the run is complete
	EventType *EventType `json:"eventType,omitempty"`
	// The set of **input** datasets.
	Inputs []InputElement `json:"inputs,omitempty"`
	Job    *Job           `json:"job,omitempty"`
	// The set of **output** datasets.
	Outputs []OutputElement `json:"outputs,omitempty"`
	Run     *RunInfo        `json:"run,omitempty"`
	Dataset *Dataset        `json:"dataset,omitempty"`
}

// A Dataset sent within static metadata events
type Dataset struct {
	// The facets for this dataset
	Facets *facets.DatasetFacets `json:"facets,omitempty"`
	// The unique name for that dataset within that namespace
	Name string `json:"name"`
	// The namespace containing that dataset
	Namespace string `json:"namespace"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a
// given tag or sha

// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
// of the schema definition for this facet

// set to true to delete a facet

// An input dataset
type InputElement struct {
	// The facets for this dataset
	Facets *facets.DatasetFacets `json:"facets,omitempty"`
	// The unique name for that dataset within that namespace
	Name string `json:"name"`
	// The namespace containing that dataset
	Namespace string `json:"namespace"`
	// The input facets for this dataset.
	InputFacets *facets.InputDatasetFacets `json:"inputFacets,omitempty"`
}

// An Input Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a
// given tag or sha

// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
// of the schema definition for this facet

type Job struct {
	// The job facets.
	Facets *facets.JobFacets `json:"facets,omitempty"`
	// The unique name for that job within that namespace
	Name string `json:"name"`
	// The namespace containing that job
	Namespace string `json:"namespace"`
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a
// given tag or sha

// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
// of the schema definition for this facet

// set to true to delete a facet

// An output dataset
type OutputElement struct {
	// The facets for this dataset
	Facets *facets.DatasetFacets `json:"facets,omitempty"`
	// The unique name for that dataset within that namespace
	Name string `json:"name"`
	// The namespace containing that dataset
	Namespace string `json:"namespace"`
	// The output facets for this dataset
	OutputFacets *facets.OutputDatasetFacets `json:"outputFacets,omitempty"`
}

// An Output Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a
// given tag or sha

// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
// of the schema definition for this facet

type RunInfo struct {
	// The run facets.
	Facets *facets.RunFacets `json:"facets,omitempty"`
	// The globally unique ID of the run associated with the job.
	RunID string `json:"runId"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a
// given tag or sha

// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
// of the schema definition for this facet

// the current transition of the run state. It is required to issue 1 START event and 1 of [
// COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be
// added to the same run. For example to send additional metadata after the run is complete
type EventType string

const (
	EventTypeAbort    EventType = "ABORT"
	EventTypeComplete EventType = "COMPLETE"
	EventTypeFail     EventType = "FAIL"
	EventTypeOther    EventType = "OTHER"
	EventTypeRunning  EventType = "RUNNING"
	EventTypeStart    EventType = "START"
)
