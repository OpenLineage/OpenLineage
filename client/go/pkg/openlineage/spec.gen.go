package openlineage

import "github.com/ThijsKoot/openlineage/client/go/pkg/facets"

type Event struct {
	EventTime string          `json:"eventTime"`           // the time the event occurred at
	Producer  string          `json:"producer"`            // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string          `json:"schemaURL"`           // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this RunEvent
	EventType *EventType      `json:"eventType,omitempty"` // the current transition of the run state. It is required to issue 1 START event and 1 of [; COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be; added to the same run. For example to send additional metadata after the run is complete
	Inputs    []InputElement  `json:"inputs,omitempty"`    // The set of **input** datasets.
	Job       *Job            `json:"job,omitempty"`
	Outputs   []OutputElement `json:"outputs,omitempty"` // The set of **output** datasets.
	Run       *RunInfo        `json:"run,omitempty"`
	Dataset   *Dataset        `json:"dataset,omitempty"`
}

// A Dataset sent within static metadata events
type Dataset struct {
	Facets    *facets.DatasetFacets `json:"facets,omitempty"` // The facets for this dataset
	Name      string                `json:"name"`             // The unique name for that dataset within that namespace
	Namespace string                `json:"namespace"`        // The namespace containing that dataset
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
// set to true to delete a facet

// An input dataset
type InputElement struct {
	Facets      *facets.DatasetFacets      `json:"facets,omitempty"`      // The facets for this dataset
	Name        string                     `json:"name"`                  // The unique name for that dataset within that namespace
	Namespace   string                     `json:"namespace"`             // The namespace containing that dataset
	InputFacets *facets.InputDatasetFacets `json:"inputFacets,omitempty"` // The input facets for this dataset.
}

// An Input Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet

type Job struct {
	Facets    *facets.JobFacets `json:"facets,omitempty"` // The job facets.
	Name      string            `json:"name"`             // The unique name for that job within that namespace
	Namespace string            `json:"namespace"`        // The namespace containing that job
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
// set to true to delete a facet

// An output dataset
type OutputElement struct {
	Facets       *facets.DatasetFacets       `json:"facets,omitempty"`       // The facets for this dataset
	Name         string                      `json:"name"`                   // The unique name for that dataset within that namespace
	Namespace    string                      `json:"namespace"`              // The namespace containing that dataset
	OutputFacets *facets.OutputDatasetFacets `json:"outputFacets,omitempty"` // The output facets for this dataset
}

// An Output Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet

type RunInfo struct {
	Facets *facets.RunFacets `json:"facets,omitempty"` // The run facets.
	RunID  string            `json:"runId"`            // The globally unique ID of the run associated with the job.
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets

// URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet

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
