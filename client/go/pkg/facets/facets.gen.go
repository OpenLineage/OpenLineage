package facets

import (
	"time"
)

// An Input Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
//
// An Output Dataset Facet
type Subset struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL       string               `json:"_schemaURL"`
	InputCondition  *InputConditionClass `json:"inputCondition,omitempty"`
	OutputCondition *InputConditionClass `json:"outputCondition,omitempty"`
}

// The condition to define a subset
type InputConditionClass struct {
	Locations  []string           `json:"locations,omitempty"`
	Type       InputConditionType `json:"type"`
	Partitions []Partition        `json:"partitions,omitempty"`
	Left       *LeftClass         `json:"left,omitempty"`
	// Allowed values: 'AND' or 'OR'
	Operator *string    `json:"operator,omitempty"`
	Right    *LeftClass `json:"right,omitempty"`
	// Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN',
	// 'LESS_EQUAL_THAN'
	Comparison *string `json:"comparison,omitempty"`
}

// The condition to define a subset
type LeftClass struct {
	Locations  []string    `json:"locations,omitempty"`
	Type       LeftType    `json:"type"`
	Partitions []Partition `json:"partitions,omitempty"`
	Left       *LeftClass  `json:"left,omitempty"`
	// Allowed values: 'AND' or 'OR'
	Operator *string    `json:"operator,omitempty"`
	Right    *LeftClass `json:"right,omitempty"`
	// Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN',
	// 'LESS_EQUAL_THAN'
	Comparison *string `json:"comparison,omitempty"`
	Field      *string `json:"field,omitempty"`
	Value      *string `json:"value,omitempty"`
}

type Partition struct {
	Dimensions map[string]interface{} `json:"dimensions"`
	// Optionally provided identifier of the partition specified
	Identifier *string `json:"identifier,omitempty"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Catalog struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// Additional catalog properties
	CatalogProperties map[string]string `json:"catalogProperties,omitempty"`
	// The storage framework for which the catalog is configured
	Framework string `json:"framework"`
	// URI or connection string to the catalog, if applicable.
	MetadataURI *string `json:"metadataUri,omitempty"`
	// Name of the catalog, as configured in the source system.
	Name string `json:"name"`
	// Source system where the catalog is configured.
	Source *string `json:"source,omitempty"`
	// Type of the catalog.
	Type string `json:"type"`
	// URI or connection string to the physical location of the data that the catalog describes.
	WarehouseURI *string `json:"warehouseUri,omitempty"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ColumnLineage struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// Column level lineage that affects the whole dataset. This includes filtering, sorting,
	// grouping (aggregates), joining, window functions, etc.
	Dataset []DatasetElement `json:"dataset,omitempty"`
	// Column level lineage that maps output fields into input fields used to evaluate them.
	Fields map[string]FieldValue `json:"fields"`
}

// Represents a single dependency on some field (column).
type DatasetElement struct {
	// The input field
	Field string `json:"field"`
	// The input dataset name
	Name string `json:"name"`
	// The input dataset namespace
	Namespace       string           `json:"namespace"`
	Transformations []Transformation `json:"transformations,omitempty"`
}

type Transformation struct {
	// a string representation of the transformation applied
	Description *string `json:"description,omitempty"`
	// is transformation masking the data or not
	Masking *bool `json:"masking,omitempty"`
	// The subtype of the transformation
	Subtype *string `json:"subtype,omitempty"`
	// The type of the transformation. Allowed values are: DIRECT, INDIRECT
	Type string `json:"type"`
}

type FieldValue struct {
	InputFields []DatasetElement `json:"inputFields"`
	// a string representation of the transformation applied
	TransformationDescription *string `json:"transformationDescription,omitempty"`
	// IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input;
	// MASKED: no original data available (like a hash of PII for example)
	TransformationType *string `json:"transformationType,omitempty"`
}

// list of tests performed on dataset or dataset columns, and their results
//
// # An Input Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DataQualityAssertions struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL  string      `json:"_schemaURL"`
	Assertions []Assertion `json:"assertions"`
}

type Assertion struct {
	// Type of expectation test that dataset is subjected to
	Assertion string `json:"assertion"`
	// Column that expectation is testing. It should match the name provided in
	// SchemaDatasetFacet. If column field is empty, then expectation refers to whole dataset.
	Column *string `json:"column,omitempty"`
	// The configured severity level of the assertion. Common values are 'error' (test failure
	// blocks pipeline) or 'warn' (test failure produces warning only).
	Severity *string `json:"severity,omitempty"`
	Success  bool    `json:"success"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DataQualityMetricsDatasetFacetDataQualityMetrics struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// The size in bytes
	Bytes *int64 `json:"bytes,omitempty"`
	// The property key is the column name
	ColumnMetrics map[string]PurpleColumnMetric `json:"columnMetrics"`
	// The number of files evaluated
	FileCount *int64 `json:"fileCount,omitempty"`
	// The last time the dataset was changed
	LastUpdated *time.Time `json:"lastUpdated,omitempty"`
	// The number of rows evaluated
	RowCount *int64 `json:"rowCount,omitempty"`
}

type PurpleColumnMetric struct {
	// The number of values in this column
	Count *float64 `json:"count,omitempty"`
	// The number of distinct values in this column for the rows evaluated
	DistinctCount *int64   `json:"distinctCount,omitempty"`
	Max           *float64 `json:"max,omitempty"`
	Min           *float64 `json:"min,omitempty"`
	// The number of null values in this column for the rows evaluated
	NullCount *int64 `json:"nullCount,omitempty"`
	// The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
	Quantiles map[string]float64 `json:"quantiles,omitempty"`
	// The total sum of values in this column for the rows evaluated
	Sum *float64 `json:"sum,omitempty"`
}

// An Input Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DataQualityMetricsInputDatasetFacetDataQualityMetrics struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// The size in bytes
	Bytes *int64 `json:"bytes,omitempty"`
	// The property key is the column name
	ColumnMetrics map[string]FluffyColumnMetric `json:"columnMetrics"`
	// The number of files evaluated
	FileCount *int64 `json:"fileCount,omitempty"`
	// The last time the dataset was changed
	LastUpdated *time.Time `json:"lastUpdated,omitempty"`
	// The number of rows evaluated
	RowCount *int64 `json:"rowCount,omitempty"`
}

type FluffyColumnMetric struct {
	// The number of values in this column
	Count *float64 `json:"count,omitempty"`
	// The number of distinct values in this column for the rows evaluated
	DistinctCount *int64   `json:"distinctCount,omitempty"`
	Max           *float64 `json:"max,omitempty"`
	Min           *float64 `json:"min,omitempty"`
	// The number of null values in this column for the rows evaluated
	NullCount *int64 `json:"nullCount,omitempty"`
	// The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
	Quantiles map[string]float64 `json:"quantiles,omitempty"`
	// The total sum of values in this column for the rows evaluated
	Sum *float64 `json:"sum,omitempty"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DatasetType struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// Dataset type, for example: TABLE|VIEW|FILE|TOPIC|STREAM|MODEL|JOB_OUTPUT.
	DatasetType string `json:"datasetType"`
	// Optional sub-type within the dataset type (e.g., MATERIALIZED, EXTERNAL, TEMPORARY).
	SubType *string `json:"subType,omitempty"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Version struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// The version of the dataset.
	DatasetVersion string `json:"datasetVersion"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DataSource struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool   `json:"_deleted,omitempty"`
	Name    *string `json:"name,omitempty"`
	URI     *string `json:"uri,omitempty"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DocumentationDatasetFacetDocumentation struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// MIME type of the description field content.
	ContentType *string `json:"contentType,omitempty"`
	// The description of the dataset.
	Description string `json:"description"`
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DocumentationJobFacetDocumentation struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// MIME type of the description field content.
	ContentType *string `json:"contentType,omitempty"`
	// The description of the job.
	Description string `json:"description"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type EnvironmentVariables struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// The environment variables for the run.
	EnvironmentVariables []EnvironmentVariableElement `json:"environmentVariables"`
}

type EnvironmentVariableElement struct {
	// The name of the environment variable.
	Name string `json:"name"`
	// The value of the environment variable.
	Value string `json:"value"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ErrorMessage struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// A human-readable string representing error message generated by observed system
	Message string `json:"message"`
	// Programming language the observed system uses.
	ProgrammingLanguage string `json:"programmingLanguage"`
	// A language-specific stack trace generated by observed system
	StackTrace *string `json:"stackTrace,omitempty"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ExecutionParameters struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// The parameters passed to the Job at runtime
	Parameters []ParameterElement `json:"parameters,omitempty"`
}

type ParameterElement struct {
	// Human-readable description of the property.
	Description *string `json:"description,omitempty"`
	// Unique identifier of the property.
	Key string `json:"key"`
	// Human-readable name of the property.
	Name *string `json:"name,omitempty"`
	// Value of the property.
	Value *string `json:"value,omitempty"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ExternalQuery struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// Identifier for the external system
	ExternalQueryID string `json:"externalQueryId"`
	// source of the external query
	Source string `json:"source"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ExtractionError struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string  `json:"_schemaURL"`
	Errors    []Error `json:"errors"`
	// The number of distinguishable tasks in a run that were processed not successfully by
	// OpenLineage. Those could be, for example, distinct SQL statements.
	FailedTasks int64 `json:"failedTasks"`
	// The number of distinguishable tasks in a run that were processed by OpenLineage, whether
	// successfully or not. Those could be, for example, distinct SQL statements.
	TotalTasks int64 `json:"totalTasks"`
}

type Error struct {
	// Text representation of extraction error message.
	ErrorMessage string `json:"errorMessage"`
	// Stack trace of extraction error message
	StackTrace *string `json:"stackTrace,omitempty"`
	// Text representation of task that failed. This can be, for example, SQL statement that
	// parser could not interpret.
	Task *string `json:"task,omitempty"`
	// Order of task (counted from 0).
	TaskNumber *int64 `json:"taskNumber,omitempty"`
}

// An Input Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type InputStatistics struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// The number of files read
	FileCount *int64 `json:"fileCount,omitempty"`
	// The number of rows read
	RowCount *int64 `json:"rowCount,omitempty"`
	// The size in bytes read
	Size *int64 `json:"size,omitempty"`
}

// Maps execution dependencies (control flow relationships) between upstream and downstream
// job runs
//
// # A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type JobDependencies struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// Job runs that will start after completion of the current run.
	Downstream []DownstreamElement `json:"downstream,omitempty"`
	// Specifies the condition under which this job will run based on the status of upstream
	// jobs.
	TriggerRule *string `json:"trigger_rule,omitempty"`
	// Job runs that must complete before the current run can start.
	Upstream []DownstreamElement `json:"upstream,omitempty"`
}

// Used to store all information about job dependency (e.g., job, run, etc.).
type DownstreamElement struct {
	// Used to describe whether the upstream job directly triggers the downstream job, or
	// whether the dependency is implicit (e.g. time-based).
	DependencyType *string        `json:"dependency_type,omitempty"`
	Job            DownstreamJob  `json:"job"`
	Run            *DownstreamRun `json:"run,omitempty"`
	// Used to describe the exact sequence condition on which the downstream job can be executed
	// (FINISH_TO_START - downstream job can start when upstream finished; FINISH_TO_FINISH -
	// job executions can overlap, but need to finish in specified order; START_TO_START - jobs
	// need to start at the same time in parallel).
	SequenceTriggerRule *string `json:"sequence_trigger_rule,omitempty"`
	// Used to describe if the downstream job should be run based on the status of the upstream
	// job.
	StatusTriggerRule *string `json:"status_trigger_rule,omitempty"`
}

// Used to store information about job (e.g., namespace and name).
type DownstreamJob struct {
	// The unique name of a job within that namespace
	Name string `json:"name"`
	// The namespace containing the job
	Namespace string `json:"namespace"`
}

// Used to store information about run (e.g., runId).
type DownstreamRun struct {
	// The globally unique ID of the run.
	RunID string `json:"runId"`
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type JobType struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// OpenLineage integration type of this job: for example SPARK|DBT|AIRFLOW|FLINK
	Integration string `json:"integration"`
	// Run type, for example: QUERY|COMMAND|DAG|TASK|JOB|MODEL. This is an integration-specific
	// field.
	JobType *string `json:"jobType,omitempty"`
	// Job processing type like: BATCH or STREAMING
	ProcessingType string `json:"processingType"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type LifecycleStateChange struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// The lifecycle state change.
	LifecycleStateChange LifecycleStateChangeEnum `json:"lifecycleStateChange"`
	// Previous name of the dataset in case of renaming it.
	PreviousIdentifier *PreviousIdentifier `json:"previousIdentifier,omitempty"`
}

// Previous name of the dataset in case of renaming it.
type PreviousIdentifier struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type NominalTime struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal
	// end time (excluded) of the run. (Should be the nominal start time of the next run)
	NominalEndTime *time.Time `json:"nominalEndTime,omitempty"`
	// An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal
	// start time (included) of the run. AKA the schedule time
	NominalStartTime time.Time `json:"nominalStartTime"`
}

// An Output Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type OutputStatistics struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// The number of files written to the dataset
	FileCount *int64 `json:"fileCount,omitempty"`
	// The number of rows written to the dataset
	RowCount *int64 `json:"rowCount,omitempty"`
	// The size in bytes written to the dataset
	Size *int64 `json:"size,omitempty"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type OwnershipDatasetFacetOwnership struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// The owners of the dataset.
	Owners []PurpleOwner `json:"owners,omitempty"`
}

type PurpleOwner struct {
	// the identifier of the owner of the Dataset. It is recommended to define this as a URN.
	// For example application:foo, user:jdoe, team:data
	Name string `json:"name"`
	// The type of ownership (optional)
	Type *string `json:"type,omitempty"`
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type OwnershipJobFacetOwnership struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// The owners of the job.
	Owners []FluffyOwner `json:"owners,omitempty"`
}

type FluffyOwner struct {
	// the identifier of the owner of the Job. It is recommended to define this as a URN. For
	// example application:foo, user:jdoe, team:data
	Name string `json:"name"`
	// The type of ownership (optional)
	Type *string `json:"type,omitempty"`
}

// the id of the parent run and job, iff this run was spawn from an other run (for example,
// the Dag run scheduling its tasks)
//
// # A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Parent struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string    `json:"_schemaURL"`
	Job       ParentJob `json:"job"`
	Root      *Root     `json:"root,omitempty"`
	Run       ParentRun `json:"run"`
}

type ParentJob struct {
	// The unique name for that job within that namespace
	Name string `json:"name"`
	// The namespace containing that job
	Namespace string `json:"namespace"`
}

type Root struct {
	Job RootJob `json:"job"`
	Run RootRun `json:"run"`
}

type RootJob struct {
	// The unique name containing root job within that namespace
	Name string `json:"name"`
	// The namespace containing root job
	Namespace string `json:"namespace"`
}

type RootRun struct {
	// The globally unique ID of the root run associated with the root job.
	RunID string `json:"runId"`
}

type ParentRun struct {
	// The globally unique ID of the run associated with the job.
	RunID string `json:"runId"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ProcessingEngine struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// Processing engine name, e.g. Airflow or Spark
	Name *string `json:"name,omitempty"`
	// OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration
	// package version
	OpenlineageAdapterVersion *string `json:"openlineageAdapterVersion,omitempty"`
	// Processing engine version. Might be Airflow or Spark version.
	Version string `json:"version"`
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type SQL struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool   `json:"_deleted,omitempty"`
	Dialect *string `json:"dialect,omitempty"`
	Query   string  `json:"query"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Schema struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// The fields of the data source.
	Fields []FieldElement `json:"fields,omitempty"`
}

type FieldElement struct {
	// The description of the field.
	Description *string `json:"description,omitempty"`
	// Nested struct fields.
	Fields []FieldElement `json:"fields,omitempty"`
	// The name of the field.
	Name string `json:"name"`
	// The ordinal position of the field in the schema (1-indexed).
	OrdinalPosition *int64 `json:"ordinal_position,omitempty"`
	// The type of the field.
	Type *string `json:"type,omitempty"`
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type SourceCode struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// Language in which source code of this job was written.
	Language string `json:"language"`
	// Source code of this job.
	SourceCode string `json:"sourceCode"`
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type SourceCodeLocation struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// optional branch name
	Branch *string `json:"branch,omitempty"`
	// the path in the repo containing the source files
	Path *string `json:"path,omitempty"`
	// the URL to the repository
	RepoURL *string `json:"repoUrl,omitempty"`
	// optional tag name
	Tag *string `json:"tag,omitempty"`
	// the source control system
	Type string `json:"type"`
	// the full http URL to locate the file
	URL string `json:"url"`
	// the current version deployed (not a branch name, the actual unique version)
	Version *string `json:"version,omitempty"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Storage struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// File format with allowed values: parquet, orc, avro, json, csv, text, xml.
	FileFormat *string `json:"fileFormat,omitempty"`
	// Storage layer provider with allowed values: iceberg, delta.
	StorageLayer string `json:"storageLayer"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Symlinks struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted     *bool        `json:"_deleted,omitempty"`
	Identifiers []Identifier `json:"identifiers,omitempty"`
}

type Identifier struct {
	// The dataset name
	Name string `json:"name"`
	// The dataset namespace
	Namespace string `json:"namespace"`
	// Identifier type
	Type string `json:"type"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type TagsDatasetFacetTags struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// The tags applied to the dataset facet
	Tags []TagElement `json:"tags,omitempty"`
}

type TagElement struct {
	// Identifies the field in a dataset if a tag applies to one
	Field *string `json:"field,omitempty"`
	// Key that identifies the tag
	Key string `json:"key"`
	// The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
	Source *string `json:"source,omitempty"`
	// The value of the field
	Value string `json:"value"`
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type TagsJobFacetTags struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// set to true to delete a facet
	Deleted *bool `json:"_deleted,omitempty"`
	// The tags applied to the job facet
	Tags []TagClass `json:"tags,omitempty"`
}

type TagClass struct {
	// Key that identifies the tag
	Key string `json:"key"`
	// The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
	Source *string `json:"source,omitempty"`
	// The value of the field
	Value string `json:"value"`
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type TagsRunFacetTags struct {
	// URI identifying the producer of this metadata. For example this could be a git url with a
	// given tag or sha
	Producer string `json:"_producer"`
	// The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version
	// of the schema definition for this facet
	SchemaURL string `json:"_schemaURL"`
	// The tags applied to the run facet
	Tags []TagsTag `json:"tags,omitempty"`
}

type TagsTag struct {
	// Key that identifies the tag
	Key string `json:"key"`
	// The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.
	Source *string `json:"source,omitempty"`
	// The value of the field
	Value string `json:"value"`
}

type LeftType string

const (
	Field           LeftType = "field"
	Literal         LeftType = "literal"
	PurpleBinary    LeftType = "binary"
	PurpleCompare   LeftType = "compare"
	PurpleLocation  LeftType = "location"
	PurplePartition LeftType = "partition"
)

type InputConditionType string

const (
	FluffyBinary    InputConditionType = "binary"
	FluffyCompare   InputConditionType = "compare"
	FluffyLocation  InputConditionType = "location"
	FluffyPartition InputConditionType = "partition"
)

// The lifecycle state change.
type LifecycleStateChangeEnum string

const (
	Alter     LifecycleStateChangeEnum = "ALTER"
	Create    LifecycleStateChangeEnum = "CREATE"
	Drop      LifecycleStateChangeEnum = "DROP"
	Overwrite LifecycleStateChangeEnum = "OVERWRITE"
	Rename    LifecycleStateChangeEnum = "RENAME"
	Truncate  LifecycleStateChangeEnum = "TRUNCATE"
)
