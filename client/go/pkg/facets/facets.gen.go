package facets

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ColumnLineage struct {
	Producer  string           `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string           `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted   *bool            `json:"_deleted,omitempty"` // set to true to delete a facet
	Fields    map[string]Field `json:"fields"`             // Column level lineage that maps output fields into input fields used to evaluate them.
}

type Field struct {
	InputFields               []InputField `json:"inputFields"`
	TransformationDescription *string      `json:"transformationDescription,omitempty"` // a string representation of the transformation applied
	TransformationType        *string      `json:"transformationType,omitempty"`        // IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input;; MASKED: no original data available (like a hash of PII for example)
}

type InputField struct {
	Field           string           `json:"field"`     // The input field
	Name            string           `json:"name"`      // The input dataset name
	Namespace       string           `json:"namespace"` // The input dataset namespace
	Transformations []Transformation `json:"transformations,omitempty"`
}

type Transformation struct {
	Description *string `json:"description,omitempty"` // a string representation of the transformation applied
	Masking     *bool   `json:"masking,omitempty"`     // is transformation masking the data or not
	Subtype     *string `json:"subtype,omitempty"`     // The subtype of the transformation
	Type        string  `json:"type"`                  // The type of the transformation. Allowed values are: DIRECT, INDIRECT
}

// list of tests performed on dataset or dataset columns, and their results
//
// # An Input Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DataQualityAssertions struct {
	Producer   string      `json:"_producer"`  // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL  string      `json:"_schemaURL"` // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Assertions []Assertion `json:"assertions"`
}

type Assertion struct {
	Assertion string  `json:"assertion"`        // Type of expectation test that dataset is subjected to
	Column    *string `json:"column,omitempty"` // Column that expectation is testing. It should match the name provided in; SchemaDatasetFacet. If column field is empty, then expectation refers to whole dataset.
	Success   bool    `json:"success"`
}

// An Input Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DataQualityMetrics struct {
	Producer      string                  `json:"_producer"`           // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL     string                  `json:"_schemaURL"`          // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Bytes         *int64                  `json:"bytes,omitempty"`     // The size in bytes
	ColumnMetrics map[string]ColumnMetric `json:"columnMetrics"`       // The property key is the column name
	FileCount     *int64                  `json:"fileCount,omitempty"` // The number of files evaluated
	RowCount      *int64                  `json:"rowCount,omitempty"`  // The number of rows evaluated
}

type ColumnMetric struct {
	Count         *float64           `json:"count,omitempty"`         // The number of values in this column
	DistinctCount *int64             `json:"distinctCount,omitempty"` // The number of distinct values in this column for the rows evaluated
	Max           *float64           `json:"max,omitempty"`
	Min           *float64           `json:"min,omitempty"`
	NullCount     *int64             `json:"nullCount,omitempty"` // The number of null values in this column for the rows evaluated
	Quantiles     map[string]float64 `json:"quantiles,omitempty"` // The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1
	Sum           *float64           `json:"sum,omitempty"`       // The total sum of values in this column for the rows evaluated
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Version struct {
	Producer       string `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL      string `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted        *bool  `json:"_deleted,omitempty"` // set to true to delete a facet
	DatasetVersion string `json:"datasetVersion"`     // The version of the dataset.
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DataSource struct {
	Producer  string  `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string  `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted   *bool   `json:"_deleted,omitempty"` // set to true to delete a facet
	Name      *string `json:"name,omitempty"`
	URI       *string `json:"uri,omitempty"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DatasetDocumentation struct {
	Producer    string `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL   string `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted     *bool  `json:"_deleted,omitempty"` // set to true to delete a facet
	Description string `json:"description"`        // The description of the dataset.
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type JobDocumentation struct {
	Producer    string `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL   string `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted     *bool  `json:"_deleted,omitempty"` // set to true to delete a facet
	Description string `json:"description"`        // The description of the job.
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ErrorMessage struct {
	Producer            string  `json:"_producer"`            // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL           string  `json:"_schemaURL"`           // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Message             string  `json:"message"`              // A human-readable string representing error message generated by observed system
	ProgrammingLanguage string  `json:"programmingLanguage"`  // Programming language the observed system uses.
	StackTrace          *string `json:"stackTrace,omitempty"` // A language-specific stack trace generated by observed system
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ExternalQuery struct {
	Producer        string `json:"_producer"`       // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL       string `json:"_schemaURL"`      // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	ExternalQueryID string `json:"externalQueryId"` // Identifier for the external system
	Source          string `json:"source"`          // source of the external query
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ExtractionError struct {
	Producer    string  `json:"_producer"`  // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL   string  `json:"_schemaURL"` // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Errors      []Error `json:"errors"`
	FailedTasks int64   `json:"failedTasks"` // The number of distinguishable tasks in a run that were processed not successfully by; OpenLineage. Those could be, for example, distinct SQL statements.
	TotalTasks  int64   `json:"totalTasks"`  // The number of distinguishable tasks in a run that were processed by OpenLineage, whether; successfully or not. Those could be, for example, distinct SQL statements.
}

type Error struct {
	ErrorMessage string  `json:"errorMessage"`         // Text representation of extraction error message.
	StackTrace   *string `json:"stackTrace,omitempty"` // Stack trace of extraction error message
	Task         *string `json:"task,omitempty"`       // Text representation of task that failed. This can be, for example, SQL statement that; parser could not interpret.
	TaskNumber   *int64  `json:"taskNumber,omitempty"` // Order of task (counted from 0).
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type JobType struct {
	Producer       string  `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL      string  `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted        *bool   `json:"_deleted,omitempty"` // set to true to delete a facet
	Integration    string  `json:"integration"`        // OpenLineage integration type of this job: for example SPARK|DBT|AIRFLOW|FLINK
	JobType        *string `json:"jobType,omitempty"`  // Run type, for example: QUERY|COMMAND|DAG|TASK|JOB|MODEL. This is an integration-specific; field.
	ProcessingType string  `json:"processingType"`     // Job processing type like: BATCH or STREAMING
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type LifecycleStateChange struct {
	Producer             string                   `json:"_producer"`                    // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL            string                   `json:"_schemaURL"`                   // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted              *bool                    `json:"_deleted,omitempty"`           // set to true to delete a facet
	LifecycleStateChange LifecycleStateChangeEnum `json:"lifecycleStateChange"`         // The lifecycle state change.
	PreviousIdentifier   *PreviousIdentifier      `json:"previousIdentifier,omitempty"` // Previous name of the dataset in case of renaming it.
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
	Producer         string  `json:"_producer"`                // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL        string  `json:"_schemaURL"`               // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	NominalEndTime   *string `json:"nominalEndTime,omitempty"` // An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal; end time (excluded) of the run. (Should be the nominal start time of the next run)
	NominalStartTime string  `json:"nominalStartTime"`         // An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal; start time (included) of the run. AKA the schedule time
}

// An Output Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type OutputStatistics struct {
	Producer  string `json:"_producer"`           // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string `json:"_schemaURL"`          // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	FileCount *int64 `json:"fileCount,omitempty"` // The number of files written to the dataset
	RowCount  *int64 `json:"rowCount,omitempty"`  // The number of rows written to the dataset
	Size      *int64 `json:"size,omitempty"`      // The size in bytes written to the dataset
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type DatasetOwnership struct {
	Producer  string         `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string         `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted   *bool          `json:"_deleted,omitempty"` // set to true to delete a facet
	Owners    []DatasetOwner `json:"owners,omitempty"`   // The owners of the dataset.
}

type DatasetOwner struct {
	Name string  `json:"name"`           // the identifier of the owner of the Dataset. It is recommended to define this as a URN.; For example application:foo, user:jdoe, team:data
	Type *string `json:"type,omitempty"` // The type of ownership (optional)
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type JobOwnership struct {
	Producer  string     `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string     `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted   *bool      `json:"_deleted,omitempty"` // set to true to delete a facet
	Owners    []JobOwner `json:"owners,omitempty"`   // The owners of the job.
}

type JobOwner struct {
	Name string  `json:"name"`           // the identifier of the owner of the Job. It is recommended to define this as a URN. For; example application:foo, user:jdoe, team:data
	Type *string `json:"type,omitempty"` // The type of ownership (optional)
}

// the id of the parent run and job, iff this run was spawn from an other run (for example,
// the Dag run scheduling its tasks)
//
// # A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Parent struct {
	Producer  string `json:"_producer"`  // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string `json:"_schemaURL"` // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Job       Job    `json:"job"`
	Run       Run    `json:"run"`
}

type Job struct {
	Name      string `json:"name"`      // The unique name for that job within that namespace
	Namespace string `json:"namespace"` // The namespace containing that job
}

type Run struct {
	RunID string `json:"runId"` // The globally unique ID of the run associated with the job.
}

// A Run Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type ProcessingEngine struct {
	Producer                  string  `json:"_producer"`                           // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL                 string  `json:"_schemaURL"`                          // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Name                      *string `json:"name,omitempty"`                      // Processing engine name, e.g. Airflow or Spark
	OpenlineageAdapterVersion *string `json:"openlineageAdapterVersion,omitempty"` // OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration; package version
	Version                   string  `json:"version"`                             // Processing engine version. Might be Airflow or Spark version.
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Schema struct {
	Producer  string         `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string         `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted   *bool          `json:"_deleted,omitempty"` // set to true to delete a facet
	Fields    []FieldElement `json:"fields,omitempty"`   // The fields of the data source.
}

type FieldElement struct {
	Description *string        `json:"description,omitempty"` // The description of the field.
	Fields      []FieldElement `json:"fields,omitempty"`      // Nested struct fields.
	Name        string         `json:"name"`                  // The name of the field.
	Type        *string        `json:"type,omitempty"`        // The type of the field.
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type SourceCode struct {
	Producer   string `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL  string `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted    *bool  `json:"_deleted,omitempty"` // set to true to delete a facet
	Language   string `json:"language"`           // Language in which source code of this job was written.
	SourceCode string `json:"sourceCode"`         // Source code of this job.
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type SourceCodeLocation struct {
	Producer  string  `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string  `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted   *bool   `json:"_deleted,omitempty"` // set to true to delete a facet
	Branch    *string `json:"branch,omitempty"`   // optional branch name
	Path      *string `json:"path,omitempty"`     // the path in the repo containing the source files
	RepoURL   *string `json:"repoUrl,omitempty"`  // the URL to the repository
	Tag       *string `json:"tag,omitempty"`      // optional tag name
	Type      string  `json:"type"`               // the source control system
	URL       string  `json:"url"`                // the full http URL to locate the file
	Version   *string `json:"version,omitempty"`  // the current version deployed (not a branch name, the actual unique version)
}

// A Job Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type SQL struct {
	Producer  string `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL string `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted   *bool  `json:"_deleted,omitempty"` // set to true to delete a facet
	Query     string `json:"query"`
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Storage struct {
	Producer     string  `json:"_producer"`            // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL    string  `json:"_schemaURL"`           // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted      *bool   `json:"_deleted,omitempty"`   // set to true to delete a facet
	FileFormat   *string `json:"fileFormat,omitempty"` // File format with allowed values: parquet, orc, avro, json, csv, text, xml.
	StorageLayer string  `json:"storageLayer"`         // Storage layer provider with allowed values: iceberg, delta.
}

// A Dataset Facet
//
// all fields of the base facet are prefixed with _ to avoid name conflicts in facets
type Symlinks struct {
	Producer    string       `json:"_producer"`          // URI identifying the producer of this metadata. For example this could be a git url with a; given tag or sha
	SchemaURL   string       `json:"_schemaURL"`         // The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version; of the schema definition for this facet
	Deleted     *bool        `json:"_deleted,omitempty"` // set to true to delete a facet
	Identifiers []Identifier `json:"identifiers,omitempty"`
}

type Identifier struct {
	Name      string `json:"name"`      // The dataset name
	Namespace string `json:"namespace"` // The dataset namespace
	Type      string `json:"type"`      // Identifier type
}

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
