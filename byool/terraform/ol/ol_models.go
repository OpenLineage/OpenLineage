/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import "github.com/hashicorp/terraform-plugin-framework/types"

// ============================================================================
// OL Config — Job identity
// ============================================================================

// OLJobConfig is the OpenLineage job identity — namespace + name uniquely
// identify a job across all OL-compatible systems.
// This is the minimal required config for any OL event.
type OLJobConfig struct {
	Namespace          types.String             `tfsdk:"namespace"`
	Name               types.String             `tfsdk:"name"`
	Description        types.String             `tfsdk:"description"`
	JobType            *JobTypeModel            `tfsdk:"job_type"`
	Ownership          *JobOwnershipModel       `tfsdk:"ownership"`
	Documentation      *JobDocumentationModel   `tfsdk:"documentation"`
	SourceCode         *SourceCodeModel         `tfsdk:"source_code"`
	SourceCodeLocation *SourceCodeLocationModel `tfsdk:"source_code_location"`
	SQL                *SQLModel                `tfsdk:"sql"`
	Tags               []JobTagModel            `tfsdk:"tags"`
}

// OLInputModel and OLOutputModel are defined in models.go (they embed DatasetModel).

// DatasetModel represents a single dataset in the Terraform config.
type DatasetModel struct {
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`

	Symlinks             []SymlinkModel             `tfsdk:"symlinks"`
	Schema               *SchemaModel               `tfsdk:"schema"`
	DataSource           *DataSourceModel           `tfsdk:"data_source"`
	Documentation        *DatasetDocumentationModel `tfsdk:"documentation"`
	DatasetType          *DatasetTypeModel          `tfsdk:"dataset_type"`
	Version              *DatasetVersionModel       `tfsdk:"version"`
	Storage              *StorageModel              `tfsdk:"storage"`
	Ownership            *DatasetOwnershipModel     `tfsdk:"ownership"`
	LifecycleStateChange *LifecycleStateChangeModel `tfsdk:"lifecycle_state_change"`
	Hierarchy            *HierarchyModel            `tfsdk:"hierarchy"`
	Catalog              *CatalogModel              `tfsdk:"catalog"`
	Tags                 []DatasetTagModel          `tfsdk:"tags"`
}

// ============================================================================
// OL Config — Job facets
// These attach to the Job part of a RunEvent (or a JobEvent).
// ============================================================================

// JobTypeModel — facets.JobType
// Classifies the job by processing style, integration, and type.
type JobTypeModel struct {
	ProcessingType types.String `tfsdk:"processing_type"` // "BATCH" or "STREAMING"
	Integration    types.String `tfsdk:"integration"`     // e.g. "SPARK", "AIRFLOW", "DBT"
	JobType        types.String `tfsdk:"job_type"`        // e.g. "QUERY", "DAG", "TASK", "JOB"
}

// JobOwnershipModel — facets.OwnershipJobFacetOwnership
// Lists the owners of this job.
type JobOwnershipModel struct {
	Owners []JobOwnerModel `tfsdk:"owners"`
}

// JobOwnerModel — facets.FluffyOwner
// A single owner entry.
type JobOwnerModel struct {
	Name types.String `tfsdk:"name"` // e.g. "team:data-engineering"
	Type types.String `tfsdk:"type"` // e.g. "MAINTAINER", "OWNER", "STEWARD"
}

// JobDocumentationModel — facets.DocumentationJobFacetDocumentation
// Human-readable documentation for this job.
type JobDocumentationModel struct {
	Description types.String `tfsdk:"description"`
}

// SourceCodeModel — facets.SourceCode
// Points to the source code that implements this job.
type SourceCodeModel struct {
	Language   types.String `tfsdk:"language"`    // e.g. "Python", "Scala"
	SourceCode types.String `tfsdk:"source_code"` // the actual code or a reference
}

// SourceCodeLocationModel — facets.SourceCodeLocation
// Locates the source code in a VCS.
type SourceCodeLocationModel struct {
	Type    types.String `tfsdk:"type"`     // e.g. "git"
	URL     types.String `tfsdk:"url"`      // e.g. "https://github.com/org/repo"
	RepoURL types.String `tfsdk:"repo_url"` // optional
	Path    types.String `tfsdk:"path"`     // optional path within the repo
	Version types.String `tfsdk:"version"`  // optional commit/tag/branch
	Tag     types.String `tfsdk:"tag"`      // optional VCS tag
	Branch  types.String `tfsdk:"branch"`   // optional branch name
}

// SQLModel — facets.SQL
// The SQL query executed by this job.
type SQLModel struct {
	Query types.String `tfsdk:"query"`
}

// JobTagModel — facets.TagsJobFacetTags / TagClass
// A free-form tag attached to this job.
type JobTagModel struct {
	Name        types.String `tfsdk:"name"`
	Value       types.String `tfsdk:"value"`
	Description types.String `tfsdk:"description"` // optional
}

// ============================================================================
// OL Config — Dataset facets (shared by inputs and outputs)
// These attach to the DatasetFacets part of an InputElement or OutputElement.
// ============================================================================

// SymlinkModel — facets.Symlinks / Identifier
// An alternate name/namespace this dataset is also known as.
type SymlinkModel struct {
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`
	Type      types.String `tfsdk:"type"` // e.g. "TABLE", "VIEW"
}

// SchemaModel — facets.Schema
// The schema (column definitions) of this dataset.
type SchemaModel struct {
	Fields []SchemaFieldModel `tfsdk:"fields"`
}

// SchemaFieldModel — facets.FieldElement
// A single column/field in the dataset schema.
type SchemaFieldModel struct {
	Name        types.String `tfsdk:"name"`
	Type        types.String `tfsdk:"type"`        // data type, e.g. "VARCHAR", "INT64"
	Description types.String `tfsdk:"description"` // optional
}

// DataSourceModel — facets.DataSource
// Identifies the source system for this dataset.
type DataSourceModel struct {
	Name types.String `tfsdk:"name"` // e.g. "my-postgres"
	URI  types.String `tfsdk:"uri"`  // e.g. "postgresql://host:5432/db"
}

// DatasetDocumentationModel — facets.DocumentationDatasetFacetDocumentation
// Human-readable documentation for this dataset.
type DatasetDocumentationModel struct {
	Description types.String `tfsdk:"description"`
}

// DatasetTypeModel — facets.DatasetType
// Classifies the dataset by its storage type and format.
type DatasetTypeModel struct {
	DatasetType  types.String `tfsdk:"dataset_type"`  // e.g. "TABLE", "VIEW", "STREAM"
	MediaType    types.String `tfsdk:"media_type"`    // e.g. "application/json", optional
	StorageLayer types.String `tfsdk:"storage_layer"` // e.g. "bigquery", "hive", optional
}

// DatasetVersionModel — facets.Version
// Records the version of this dataset at the time of the run.
type DatasetVersionModel struct {
	DatasetVersion types.String `tfsdk:"dataset_version"`
}

// StorageModel — facets.Storage
// Describes the physical storage of this dataset.
type StorageModel struct {
	StorageLayer types.String `tfsdk:"storage_layer"` // e.g. "iceberg", "delta", "hive"
	FileFormat   types.String `tfsdk:"file_format"`   // e.g. "parquet", "orc", optional
}

// DatasetOwnershipModel — facets.OwnershipDatasetFacetOwnership
// Lists the owners of this dataset.
type DatasetOwnershipModel struct {
	Owners []DatasetOwnerModel `tfsdk:"owners"`
}

// DatasetOwnerModel — facets.PurpleOwner
// A single owner of a dataset.
type DatasetOwnerModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"` // optional
}

// LifecycleStateChangeModel — facets.LifecycleStateChange
// Records a state transition for this dataset (e.g. DROP, CREATE, ALTER).
type LifecycleStateChangeModel struct {
	LifecycleStateChange types.String             `tfsdk:"lifecycle_state_change"` // e.g. "DROP", "CREATE"
	PreviousIdentifier   *PreviousIdentifierModel `tfsdk:"previous_identifier"`    // optional — set on RENAME
}

// PreviousIdentifierModel — facets.PreviousIdentifier
// The old namespace+name before a RENAME lifecycle event.
type PreviousIdentifierModel struct {
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`
}

// HierarchyModel — facets.Hierarchy
// Describes the position of this dataset in a hierarchy (e.g. a partition within a table).
type HierarchyModel struct {
	Parent   HierarchyElementModel   `tfsdk:"parent"`
	Children []HierarchyElementModel `tfsdk:"children"` // optional
}

// HierarchyElementModel — facets.HierarchyElement
type HierarchyElementModel struct {
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`
	Type      types.String `tfsdk:"type"` // e.g. "TABLE", "PARTITION"
}

// CatalogModel — facets.Catalog
// Describes the catalog/metastore where this dataset is registered.
type CatalogModel struct {
	Framework    types.String `tfsdk:"framework"`     // e.g. "hive", "iceberg"
	Type         types.String `tfsdk:"type"`          // e.g. "hive"
	Name         types.String `tfsdk:"name"`          // catalog name
	MetadataURI  types.String `tfsdk:"metadata_uri"`  // e.g. "hive://localhost:9083", optional
	WarehouseURI types.String `tfsdk:"warehouse_uri"` // e.g. "hdfs://localhost/warehouse", optional
	Source       types.String `tfsdk:"source"`        // e.g. "spark", optional
}

// DatasetTagModel — facets.TagsDatasetFacetTags / TagElement
// A free-form tag attached to this dataset.
type DatasetTagModel struct {
	Name        types.String `tfsdk:"name"`
	Value       types.String `tfsdk:"value"`
	Description types.String `tfsdk:"description"` // optional
}

// ColumnLineageModel — facets.ColumnLineage
// Maps output columns back to the input columns/datasets that produced them.
type ColumnLineageModel struct {
	Fields  []ColumnLineageFieldModel   `tfsdk:"fields"`  // field-level mappings
	Dataset []ColumnLineageDatasetModel `tfsdk:"dataset"` // dataset-level mappings
}

// ColumnLineageFieldModel — facets.FieldValue (keyed by output column name)
// Maps a single output column to its contributing input fields.
type ColumnLineageFieldModel struct {
	Name        types.String      `tfsdk:"name"`        // output column name — becomes the map key
	InputFields []InputFieldModel `tfsdk:"input_field"` // which input columns feed this output column
}

// InputFieldModel — facets.DatasetElement (inside FieldValue)
// A single input column contributing to an output column.
type InputFieldModel struct {
	Namespace      types.String          `tfsdk:"namespace"`
	Name           types.String          `tfsdk:"name"`
	Field          types.String          `tfsdk:"field"`
	Transformation []TransformationModel `tfsdk:"transformation"` // optional, at most one
}

// ColumnLineageDatasetModel — facets.DatasetElement (dataset-level)
// Dataset-level contribution — input dataset feeds an output field but exact column unknown.
type ColumnLineageDatasetModel struct {
	Namespace      types.String          `tfsdk:"namespace"`
	Name           types.String          `tfsdk:"name"`
	Field          types.String          `tfsdk:"field"`
	Transformation []TransformationModel `tfsdk:"transformation"` // optional, at most one
}

// TransformationModel — facets.Transformation
// Describes how data flows from an input field to an output field.
type TransformationModel struct {
	Type        types.String `tfsdk:"type"`        // "DIRECT" or "INDIRECT" — required
	Subtype     types.String `tfsdk:"subtype"`     // e.g. "IDENTITY", "FILTER" — optional
	Description types.String `tfsdk:"description"` // optional
	Masking     types.Bool   `tfsdk:"masking"`     // true if this transform masks/anonymises data
}
