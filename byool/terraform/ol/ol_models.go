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
	Namespace          types.String                `tfsdk:"namespace"`
	Name               types.String                `tfsdk:"name"`
	Description        types.String                `tfsdk:"description"`
	JobType            *JobTypeJobModel            `tfsdk:"job_type"`
	Ownership          *OwnershipJobModel          `tfsdk:"ownership"`
	Documentation      *DocumentationJobModel      `tfsdk:"documentation"`
	SourceCode         *SourceCodeJobModel         `tfsdk:"source_code"`
	SourceCodeLocation *SourceCodeLocationJobModel `tfsdk:"source_code_location"`
	SQL                *SQLJobModel                `tfsdk:"sql"`
	Tags               []TagsJobModel              `tfsdk:"tags"`
}

// OLInputModel and OLOutputModel are defined in models.go (they embed DatasetModel).

// DatasetModel represents a single dataset in the Terraform config.
type DatasetModel struct {
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`

	Symlinks             []SymlinksDatasetModel            `tfsdk:"symlinks"`
	Schema               *SchemaDatasetModel               `tfsdk:"schema"`
	DataSource           *DataSourceDatasetModel           `tfsdk:"data_source"`
	Documentation        *DocumentationDatasetModel        `tfsdk:"documentation"`
	DatasetType          *DatasetTypeDatasetModel          `tfsdk:"dataset_type"`
	Version              *DatasetVersionDatasetModel       `tfsdk:"version"`
	Storage              *StorageDatasetModel              `tfsdk:"storage"`
	Ownership            *OwnershipDatasetModel            `tfsdk:"ownership"`
	LifecycleStateChange *LifecycleStateChangeDatasetModel `tfsdk:"lifecycle_state_change"`
	Hierarchy            *HierarchyDatasetModel            `tfsdk:"hierarchy"`
	Catalog              *CatalogDatasetModel              `tfsdk:"catalog"`
	Tags                 []TagsDatasetModel                `tfsdk:"tags"`
}

// ============================================================================
// OL Config — Job facets
// ============================================================================

// JobTypeJobModel — facets.JobTypeJobFacet
// Classifies the job by processing style, integration, and type.
type JobTypeJobModel struct {
	ProcessingType types.String `tfsdk:"processing_type"` // "BATCH" or "STREAMING"
	Integration    types.String `tfsdk:"integration"`     // e.g. "SPARK", "AIRFLOW", "DBT"
	JobType        types.String `tfsdk:"job_type"`        // e.g. "QUERY", "DAG", "TASK", "JOB"
}

// OwnershipJobModel — facets.OwnershipJobFacet
// Lists the owners of this job.
type OwnershipJobModel struct {
	Owners []JobOwnerModel `tfsdk:"owners"`
}

// JobOwnerModel — facets.FluffyOwner
// A single owner entry.
type JobOwnerModel struct {
	Name types.String `tfsdk:"name"` // e.g. "team:data-engineering"
	Type types.String `tfsdk:"type"` // e.g. "MAINTAINER", "OWNER", "STEWARD"
}

// DocumentationJobModel — facets.DocumentationJobFacet
// Human-readable documentation for this job.
type DocumentationJobModel struct {
	Description types.String `tfsdk:"description"`
}

// SourceCodeJobModel — facets.SourceCodeJobFacet
// Points to the source code that implements this job.
type SourceCodeJobModel struct {
	Language   types.String `tfsdk:"language"`    // e.g. "Python", "Scala"
	SourceCode types.String `tfsdk:"source_code"` // the actual code or a reference
}

// SourceCodeLocationJobModel — facets.SourceCodeLocationJobFacet
// Locates the source code in a VCS.
type SourceCodeLocationJobModel struct {
	Type    types.String `tfsdk:"type"`     // e.g. "git"
	URL     types.String `tfsdk:"url"`      // e.g. "https://github.com/org/repo"
	RepoURL types.String `tfsdk:"repo_url"` // optional
	Path    types.String `tfsdk:"path"`     // optional path within the repo
	Version types.String `tfsdk:"version"`  // optional commit/tag/branch
	Tag     types.String `tfsdk:"tag"`      // optional VCS tag
	Branch  types.String `tfsdk:"branch"`   // optional branch name
}

// SQLJobModel — facets.SQLJobFacet
// The SQL query executed by this job.
type SQLJobModel struct {
	Query types.String `tfsdk:"query"`
}

// TagsJobModel — facets.TagsJobFacet
// A free-form tag attached to this job.
type TagsJobModel struct {
	Name        types.String `tfsdk:"name"`
	Value       types.String `tfsdk:"value"`
	Description types.String `tfsdk:"description"` // optional
}

// ============================================================================
// OL Config — Dataset facets
// ============================================================================

// SymlinksDatasetModel — facets.SymlinksDatasetFacet
// An alternate name/namespace this dataset is also known as.
type SymlinksDatasetModel struct {
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`
	Type      types.String `tfsdk:"type"` // e.g. "TABLE", "VIEW"
}

// SchemaDatasetModel — facets.SchemaDatasetFacet
// The schema (column definitions) of this dataset.
type SchemaDatasetModel struct {
	Fields []SchemaFieldModel `tfsdk:"fields"`
}

// SchemaFieldModel — facets.FieldElement
// A single column/field in the dataset schema.
type SchemaFieldModel struct {
	Name        types.String `tfsdk:"name"`
	Type        types.String `tfsdk:"type"`        // data type, e.g. "VARCHAR", "INT64"
	Description types.String `tfsdk:"description"` // optional
}

// DataSourceDatasetModel — facets.DatasourceDatasetFacet
// Identifies the source system for this dataset.
// Note: the Go client uses 'Datasource' (lowercase s); this model uses 'DataSource' per Go convention.
type DataSourceDatasetModel struct {
	Name types.String `tfsdk:"name"` // e.g. "my-postgres"
	URI  types.String `tfsdk:"uri"`  // e.g. "postgresql://host:5432/db"
}

// DocumentationDatasetModel — facets.DocumentationDatasetFacet
// Human-readable documentation for this dataset.
type DocumentationDatasetModel struct {
	Description types.String `tfsdk:"description"`
}

// DatasetTypeDatasetModel — facets.DatasetTypeDatasetFacet
// Classifies the dataset by its storage type and format.
// Note: 'DatasetType' appears twice because the client facet is named DatasetTypeDatasetFacet.
type DatasetTypeDatasetModel struct {
	DatasetType  types.String `tfsdk:"dataset_type"`  // e.g. "TABLE", "VIEW", "STREAM"
	MediaType    types.String `tfsdk:"media_type"`    // e.g. "application/json", optional
	StorageLayer types.String `tfsdk:"storage_layer"` // e.g. "bigquery", "hive", optional
}

// DatasetVersionDatasetModel — facets.DatasetVersionDatasetFacet
// Records the version of this dataset at the time of the run.
// Note: 'Dataset' appears twice because the client facet is named DatasetVersionDatasetFacet.
type DatasetVersionDatasetModel struct {
	DatasetVersion types.String `tfsdk:"dataset_version"`
}

// StorageDatasetModel — facets.StorageDatasetFacet
// Describes the physical storage of this dataset.
type StorageDatasetModel struct {
	StorageLayer types.String `tfsdk:"storage_layer"` // e.g. "iceberg", "delta", "hive"
	FileFormat   types.String `tfsdk:"file_format"`   // e.g. "parquet", "orc", optional
}

// OwnershipDatasetModel — facets.OwnershipDatasetFacet
// Lists the owners of this dataset.
type OwnershipDatasetModel struct {
	Owners []DatasetOwnerModel `tfsdk:"owners"`
}

// DatasetOwnerModel — facets.PurpleOwner
// A single owner of a dataset.
type DatasetOwnerModel struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"` // optional
}

// LifecycleStateChangeDatasetModel — facets.LifecycleStateChangeDatasetFacet
// Records a state transition for this dataset (e.g. DROP, CREATE, ALTER).
type LifecycleStateChangeDatasetModel struct {
	LifecycleStateChange types.String             `tfsdk:"lifecycle_state_change"` // e.g. "DROP", "CREATE"
	PreviousIdentifier   *PreviousIdentifierModel `tfsdk:"previous_identifier"`    // optional — set on RENAME
}

// PreviousIdentifierModel — facets.PreviousIdentifier
// The old namespace+name before a RENAME lifecycle event.
type PreviousIdentifierModel struct {
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`
}

// HierarchyDatasetModel — facets.HierarchyDatasetFacet
// Describes the position of this dataset in a hierarchy (e.g. a partition within a table).
type HierarchyDatasetModel struct {
	Parent   HierarchyElementModel   `tfsdk:"parent"`
	Children []HierarchyElementModel `tfsdk:"children"` // optional
}

// HierarchyElementModel — facets.HierarchyElement
type HierarchyElementModel struct {
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`
	Type      types.String `tfsdk:"type"` // e.g. "TABLE", "PARTITION"
}

// CatalogDatasetModel — facets.CatalogDatasetFacet
// Describes the catalog/metastore where this dataset is registered.
type CatalogDatasetModel struct {
	Framework    types.String `tfsdk:"framework"`     // e.g. "hive", "iceberg"
	Type         types.String `tfsdk:"type"`          // e.g. "hive"
	Name         types.String `tfsdk:"name"`          // catalog name
	MetadataURI  types.String `tfsdk:"metadata_uri"`  // e.g. "hive://localhost:9083", optional
	WarehouseURI types.String `tfsdk:"warehouse_uri"` // e.g. "hdfs://localhost/warehouse", optional
	Source       types.String `tfsdk:"source"`        // e.g. "spark", optional
}

// TagsDatasetModel — facets.TagsDatasetFacet
// A free-form tag attached to this dataset.
type TagsDatasetModel struct {
	Name        types.String `tfsdk:"name"`
	Value       types.String `tfsdk:"value"`
	Description types.String `tfsdk:"description"` // optional
}

// ColumnLineageDatasetModel — facets.ColumnLineageDatasetFacet
// Maps output columns back to the input columns/datasets that produced them.
type ColumnLineageDatasetModel struct {
	Fields  []ColumnLineageFieldModel          `tfsdk:"fields"`  // field-level mappings
	Dataset []ColumnLineageDatasetElementModel `tfsdk:"dataset"` // dataset-level mappings
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

// ColumnLineageDatasetElementModel — facets.DatasetElement (dataset-level)
// Dataset-level contribution — input dataset feeds an output field but exact column unknown.
type ColumnLineageDatasetElementModel struct {
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
