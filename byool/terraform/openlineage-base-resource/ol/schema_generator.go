/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/mapvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/objectvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// GenerateJobSchema builds the Terraform schema for an openlineage_job resource.
// Facets enabled in the JobCapability are included as normal (required/optional).
// Facets not enabled in the JobCapability are included as no-op stubs: every
// attribute inside them is Optional+Computed so a config copied from another
// consumer is accepted without error, but the values are silently ignored.
// Consumer-specific attributes (process_name etc.) are merged in by BaseJobResource.Schema().
func GenerateJobSchema(cap JobCapability) schema.Schema {
	blocks := map[string]schema.Block{}

	addJobBlock := func(f JobFacet, key string, full schema.Block) {
		if cap.IsEnabled(f) {
			blocks[key] = full
		} else {
			switch b := full.(type) {
			case schema.SingleNestedBlock:
				blocks[key] = stubSingleBlock(b)
			case schema.ListNestedBlock:
				blocks[key] = stubListBlock(b)
			}
		}
	}

	addJobBlock(FacetJobType, "job_type", jobTypeBlock())
	addJobBlock(FacetJobOwnership, "ownership", jobOwnershipBlock())
	addJobBlock(FacetJobDocumentation, "documentation", jobDocumentationBlock())
	addJobBlock(FacetJobSourceCode, "source_code", sourceCodeBlock())
	addJobBlock(FacetJobSourceCodeLocation, "source_code_location", sourceCodeLocationBlock())
	addJobBlock(FacetJobSQL, "sql", sqlBlock())
	addJobBlock(FacetJobTags, "tags", jobTagsBlock())

	blocks["inputs"] = inputsBlock(cap.capability)
	blocks["outputs"] = outputsBlock(cap.capability)

	return schema.Schema{
		Description: "OpenLineage job resource.",
		Attributes:  jobIdentityAttributes(),
		Blocks:      blocks,
	}
}

// GenerateDatasetSchema builds the Terraform schema for an openlineage_dataset resource.
// Only the dataset facet blocks enabled in the DatasetCapability appear as active blocks.
// Facets not enabled are included as no-op stubs so portable config is accepted without error.
// Consumer-specific attributes are merged in by BaseDatasetResource.Schema().
func GenerateDatasetSchema(cap DatasetCapability) schema.Schema {
	attrs, blocks := datasetSchema(cap.capability)

	// Start from the identity attributes, which carry RequiresReplace plan
	// modifiers on namespace and name. datasetSchema() also returns namespace
	// and name (without plan modifiers, for use in nested input/output blocks),
	// so we skip those keys here to keep datasetIdentityAttributes() authoritative.
	identityAttrs := datasetIdentityAttributes()
	for k, v := range attrs {
		if k == "namespace" || k == "name" {
			continue
		}
		identityAttrs[k] = v
	}

	return schema.Schema{
		Description: "OpenLineage dataset resource.",
		Attributes:  identityAttrs,
		Blocks:      blocks,
	}
}

// ── Job identity attributes (always present on job resources) ────────────────

func jobIdentityAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"namespace": schema.StringAttribute{
			Required:    true,
			Description: "Job namespace",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.RequiresReplace(),
			},
		},
		"name": schema.StringAttribute{
			Required:    true,
			Description: "Job name",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.RequiresReplace(),
			},
		},
		"description": optionalString("Job description"),
	}
}

// ── Dataset identity attributes (always present on dataset resources) ─────────

// datasetIdentityAttributes returns the fixed attributes every dataset resource
// has: namespace, name. These are separate from the optional facet blocks.
func datasetIdentityAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"namespace": schema.StringAttribute{
			Required:    true,
			Description: "Dataset namespace",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.RequiresReplace(),
			},
		},
		"name": schema.StringAttribute{
			Required:    true,
			Description: "Dataset name",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.RequiresReplace(),
			},
		},
	}
}

// ── Job facet blocks ──────────────────────────────────────────────────────────

func jobTypeBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Job type classification (facets.JobType)",
		Attributes: map[string]schema.Attribute{
			"processing_type": optionalString("BATCH or STREAMING"),
			"integration":     optionalString("Integration type e.g. SPARK, AIRFLOW, DBT, BYOL"),
			"job_type":        optionalString("Job type e.g. QUERY, DAG, TASK, JOB, MODEL"),
		},
		Validators: requireFields("processing_type", "integration", "job_type"),
	}
}

func jobOwnershipBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Job owners (facets.OwnershipJobFacet)",
		Blocks: map[string]schema.Block{
			"owners": schema.ListNestedBlock{
				Description: "Owner entries",
				Validators: []validator.List{
					listvalidator.SizeAtLeast(1),
				},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"name": optionalString("Owner identifier e.g. team:data-engineering"),
						"type": optionalString("Owner type e.g. MAINTAINER, OWNER, STEWARD"),
					},
					Validators: requireFields("name", "type"),
				},
			},
		},
		Validators: requireFields("owners"),
	}
}

func jobDocumentationBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Human-readable documentation for this job (facets.DocumentationJobFacet)",
		Attributes: map[string]schema.Attribute{
			"description":  optionalString("Job documentation text"),
			"content_type": optionalString("Optional MIME type for description content e.g. text/markdown"),
		},
		Validators: requireFields("description"),
	}
}

func sourceCodeBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Source code that implements this job (facets.SourceCode)",
		Attributes: map[string]schema.Attribute{
			"language":    optionalString("Programming language e.g. Python, Scala, SQL"),
			"source_code": optionalString("The source code text or a URI pointing to it"),
		},
		Validators: requireFields("language", "source_code"),
	}
}

func sourceCodeLocationBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "VCS location of the source code for this job (facets.SourceCodeLocation)",
		Attributes: map[string]schema.Attribute{
			"type":     optionalString("VCS type e.g. git"),
			"url":      optionalString("URL of the repository or file e.g. https://github.com/org/repo"),
			"repo_url": optionalString("Repository root URL when url points to a specific file"),
			"path":     optionalString("Path within the repository"),
			"version":  optionalString("Commit hash, tag, or branch name"),
			"tag":      optionalString("VCS tag"),
			"branch":   optionalString("Branch name"),
		},
		Validators: requireFields("type", "url"),
	}
}

func sqlBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "SQL query executed by this job (facets.SQL)",
		Attributes: map[string]schema.Attribute{
			"query":   optionalString("The SQL query string"),
			"dialect": optionalString("SQL dialect e.g. hive, spark, bigquery"),
		},
		Validators: requireFields("query"),
	}
}

// jobTagsBlock: value is Required (TagClass.Value is a non-pointer string in the OL spec).
// description is not part of the OL TagClass facet and is omitted.
func jobTagsBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Free-form tags attached to this job (facets.TagsJobFacet)",
		Validators: []validator.List{
			listvalidator.SizeAtLeast(1),
		},
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"name":   optionalString("Tag key"),
				"value":  optionalString("Tag value"),
				"source": optionalString("Tag source e.g. USER, INTEGRATION, DBT"),
			},
			Validators: requireFields("name", "value"),
		},
	}
}

// ── Dataset blocks (inputs / outputs) ────────────────────────────────────────

func inputsBlock(cap capability) schema.ListNestedBlock {
	attrs, blocks := datasetSchema(cap)
	return schema.ListNestedBlock{
		Description: "Input datasets",
		NestedObject: schema.NestedBlockObject{
			Attributes: attrs,
			Blocks:     blocks,
		},
	}
}

func outputsBlock(cap capability) schema.ListNestedBlock {
	attrs, blocks := datasetSchema(cap)
	if cap.isDatasetEnabled(FacetDatasetColumnLineage) {
		blocks["column_lineage"] = columnLineageBlock()
	} else {
		blocks["column_lineage"] = stubSingleBlock(columnLineageBlock())
	}
	return schema.ListNestedBlock{
		Description: "Output datasets",
		NestedObject: schema.NestedBlockObject{
			Attributes: attrs,
			Blocks:     blocks,
		},
	}
}

func datasetSchema(cap capability) (map[string]schema.Attribute, map[string]schema.Block) {
	attrs := map[string]schema.Attribute{
		"namespace": requiredString("Dataset namespace"),
		"name":      requiredString("Dataset name"),
	}
	blocks := map[string]schema.Block{}

	// Each facet is always included in the schema. When the facet is not enabled by
	// the consumer's capability, it is registered as a no-op stub (all attributes
	// Optional+Computed) so that configs shared between providers are accepted
	// without error — the values are simply ignored by the consumer.
	addBlock := func(f DatasetFacet, key string, full schema.Block) {
		if cap.isDatasetEnabled(f) {
			blocks[key] = full
		} else {
			switch b := full.(type) {
			case schema.SingleNestedBlock:
				blocks[key] = stubSingleBlock(b)
			case schema.ListNestedBlock:
				blocks[key] = stubListBlock(b)
			}
		}
	}

	addBlock(FacetDatasetSymlinks, "symlinks", symlinksBlock())
	addBlock(FacetDatasetSchema, "schema", datasetSchemaBlock())
	addBlock(FacetDatasetDataSource, "data_source", dataSourceBlock())
	addBlock(FacetDatasetDocumentation, "documentation", datasetDocumentationBlock())
	addBlock(FacetDatasetType, "dataset_type", datasetTypeBlock())
	addBlock(FacetDatasetVersion, "version", datasetVersionBlock())
	addBlock(FacetDatasetStorage, "storage", storageBlock())
	addBlock(FacetDatasetOwnership, "ownership", datasetOwnershipBlock())
	addBlock(FacetDatasetLifecycleStateChange, "lifecycle_state_change", lifecycleStateChangeBlock())
	addBlock(FacetDatasetHierarchy, "hierarchy", hierarchyBlock())
	addBlock(FacetDatasetCatalog, "catalog", catalogBlock())
	addBlock(FacetDatasetTags, "tags", datasetTagsBlock())

	return attrs, blocks
}

// ── Dataset facet blocks ──────────────────────────────────────────────────────

func symlinksBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Alternate dataset identifiers (facets.Symlinks)",
		Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"namespace": optionalString("Alternate namespace"),
				"name":      optionalString("Alternate name"),
				"type":      optionalString("e.g. TABLE, VIEW"),
			},
			Validators: requireFields("namespace", "name", "type"),
		},
	}
}

func datasetSchemaBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset schema / column definitions (facets.Schema)",
		Blocks: map[string]schema.Block{
			"fields": schema.ListNestedBlock{
				Description: "Column definitions",
				Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"name":        optionalString("Column name"),
						"type":        optionalString("Data type e.g. VARCHAR, INT64"),
						"description": optionalString("Column description"),
					},
					Validators: requireFields("name", "type"),
				},
			},
		},
		Validators: requireFields("fields"),
	}
}

func dataSourceBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Source system for this dataset (facets.DataSource)",
		Attributes: map[string]schema.Attribute{
			"name": optionalString("Source system name e.g. my-postgres"),
			"uri":  optionalString("Source system URI e.g. postgresql://host:5432/db"),
		},
		Validators: requireFields("name", "uri"),
	}
}

func datasetDocumentationBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Human-readable documentation for this dataset (facets.DocumentationDatasetFacet)",
		Attributes: map[string]schema.Attribute{
			"description":  optionalString("Dataset documentation"),
			"content_type": optionalString("Optional MIME type for description content e.g. text/markdown"),
		},
		Validators: requireFields("description"),
	}
}

func datasetTypeBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset type classification (facets.DatasetType)",
		Attributes: map[string]schema.Attribute{
			"dataset_type": optionalString("e.g. TABLE, VIEW, STREAM"),
			"sub_type":     optionalString("Optional sub-type e.g. MATERIALIZED, EXTERNAL, TEMPORARY"),
		},
		Validators: requireFields("dataset_type"),
	}
}

func datasetVersionBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset version at the time of this run (facets.Version)",
		Attributes: map[string]schema.Attribute{
			"dataset_version": optionalString("Dataset version identifier"),
		},
		Validators: requireFields("dataset_version"),
	}
}

func storageBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Physical storage of this dataset (facets.Storage)",
		Attributes: map[string]schema.Attribute{
			"storage_layer": optionalString("e.g. iceberg, delta, hive"),
			"file_format":   optionalString("e.g. parquet, orc"),
		},
		Validators: requireFields("storage_layer"),
	}
}

func datasetOwnershipBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset owners (facets.OwnershipDatasetFacet)",
		Blocks: map[string]schema.Block{
			"owners": schema.ListNestedBlock{
				Description: "Owner entries",
				Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"name": optionalString("Owner identifier"),
						"type": optionalString("Owner type e.g. MAINTAINER"),
					},
					Validators: requireFields("name", "type"),
				},
			},
		},
		Validators: requireFields("owners"),
	}
}

func lifecycleStateChangeBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset lifecycle state transition (facets.LifecycleStateChange)",
		Attributes: map[string]schema.Attribute{
			"lifecycle_state_change": optionalString("e.g. CREATE, DROP, ALTER, RENAME, OVERWRITE"),
		},
		Blocks: map[string]schema.Block{
			"previous_identifier": schema.SingleNestedBlock{
				Description: "Previous namespace+name before a RENAME",
				Attributes: map[string]schema.Attribute{
					"namespace": optionalString("Previous namespace"),
					"name":      optionalString("Previous name"),
				},
				Validators: requireFields("namespace", "name"),
			},
		},
		Validators: requireFields("lifecycle_state_change"),
	}
}

// ── Hierarchy facet blocks ───────────────────────────────────────────────────

func hierarchyBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset position in a hierarchy e.g. partition within a table (facets.Hierarchy)",
		Blocks: map[string]schema.Block{
			"hierarchy": hierarchyElementListBlock("Hierarchy levels ordered highest -> lowest"),
		},
		Validators: requireFields("hierarchy"),
	}
}

func hierarchyElementListBlock(desc string) schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: desc,
		Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"name": optionalString("Name"),
				"type": optionalString("e.g. TABLE, PARTITION"),
			},
			Validators: requireFields("name", "type"),
		},
	}
}

func catalogBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Catalog/metastore registration (facets.Catalog)",
		Attributes: map[string]schema.Attribute{
			"framework":     optionalString("e.g. hive, iceberg"),
			"type":          optionalString("Catalog type e.g. hive"),
			"name":          optionalString("Catalog name"),
			"metadata_uri":  optionalString("e.g. hive://localhost:9083"),
			"warehouse_uri": optionalString("e.g. hdfs://localhost/warehouse"),
			"source":        optionalString("Source system e.g. spark"),
			"catalog_properties": schema.MapAttribute{
				Optional:    true,
				Description: "Additional catalog-specific properties as key-value pairs",
				ElementType: types.StringType,
				Default:     mapdefault.StaticValue(types.MapNull(types.StringType)),
				Validators: []validator.Map{
					mapvalidator.KeysAre(stringvalidator.LengthAtLeast(1)),
				},
			},
		},
		Validators: requireFields("framework", "type", "name"),
	}
}

func datasetTagsBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Free-form tags on this dataset (facets.TagsDatasetFacet)",
		Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"name":   optionalString("Tag key"),
				"value":  optionalString("Tag value"),
				"source": optionalString("Tag source e.g. USER, INTEGRATION, DBT"),
				"field":  optionalString("Dataset field/column this tag applies to"),
			},
			Validators: requireFields("name", "value"),
		},
	}
}

// ── Column lineage ────────────────────────────────────────────────────────────

func columnLineageBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Column-level lineage for this output dataset (facets.ColumnLineage)",
		Blocks: map[string]schema.Block{
			"fields":  columnLineageFieldsBlock(),
			"dataset": columnLineageDatasetBlock(),
		},
		// column_lineage present → at least one fields entry must exist.
		// (A missing fields list is null, not an empty list, so AlsoRequires
		// is the right tool — listvalidator.SizeAtLeast would skip null.)
		Validators: requireFields("fields"),
	}
}

func columnLineageFieldsBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Field-level lineage: output column → input columns",
		Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"name": optionalString("Output column name"),
			},
			Blocks: map[string]schema.Block{
				"input_field": schema.ListNestedBlock{
					Description: "Input fields that contribute to this output column",
					Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
					NestedObject: schema.NestedBlockObject{
						Attributes: map[string]schema.Attribute{
							"namespace": optionalString("Input dataset namespace"),
							"name":      optionalString("Input dataset name"),
							"field":     optionalString("Input column name"),
						},
						Blocks: map[string]schema.Block{
							"transformation": transformationListBlock(),
						},
						Validators: requireFields("namespace", "name", "field"),
					},
				},
			},
			Validators: requireFields("name", "input_fields"),
		},
	}
}

func columnLineageDatasetBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Dataset-level lineage: input dataset → output field (column unknown)",
		Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"namespace": optionalString("Input dataset namespace"),
				"name":      optionalString("Input dataset name"),
				"field":     optionalString("Output field this dataset contributes to"),
			},
			Blocks: map[string]schema.Block{
				"transformation": transformationListBlock(),
			},
			Validators: requireFields(
				"namespace", "name", "field", "transformations"),
		},
	}
}

func transformationListBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "How the input data was transformed to produce the output (facets.Transformation). Multiple transformations are allowed.",
		Validators:  []validator.List{listvalidator.SizeAtLeast(1)},
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"type":        optionalString("DIRECT or INDIRECT"),
				"subtype":     optionalString("e.g. IDENTITY, AGGREGATION, FILTER"),
				"description": optionalString("Human-readable transformation description"),
				"masking": schema.BoolAttribute{
					Optional:    true,
					Description: "True if this transformation masks/anonymises data",
					Default:     booldefault.StaticBool(false),
				},
			},
			Validators: requireFields("type", "subtype"),
		},
	}
}

// ── Attribute helpers ─────────────────────────────────────────────────────────

func optionalString(desc string) schema.StringAttribute {
	return schema.StringAttribute{Optional: true, Description: desc}
}

func requiredString(desc string) schema.StringAttribute {
	return schema.StringAttribute{Required: true, Description: desc}
}

// requireFields returns an object validator slice that requires all named
// sibling attributes to be set whenever the containing block is present.
func requireFields(names ...string) []validator.Object {
	exprs := make([]path.Expression, len(names))
	for i, n := range names {
		exprs[i] = path.MatchRelative().AtName(n)
	}
	return []validator.Object{objectvalidator.AlsoRequires(exprs...)}
}

// ── Stub helpers ──────────────────────────────────────────────────────────────
//
// When a consumer has not enabled a facet (e.g. a provider that does not support
// FacetDatasetSchema), the block is still registered in the schema as a stub.
// Every attribute inside the stub is Optional+Computed with no plan modifiers —
// Terraform accepts any value written by the user but the provider never reads
// or sends it. This allows a single .tf config to be used across multiple
// providers without requiring the user to remove unsupported blocks.

// stubSingleBlock returns a copy of b where every leaf attribute is
// Optional+Computed and every nested block is also stubbed.
func stubSingleBlock(b schema.SingleNestedBlock) schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: b.Description,
		Attributes:  stubAttributes(b.Attributes),
		Blocks:      stubBlocks(b.Blocks),
	}
}

// stubListBlock returns a copy of b where every leaf attribute in its nested
// object is Optional+Computed and every nested block is also stubbed.
func stubListBlock(b schema.ListNestedBlock) schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: b.Description,
		NestedObject: schema.NestedBlockObject{
			Attributes: stubAttributes(b.NestedObject.Attributes),
			Blocks:     stubBlocks(b.NestedObject.Blocks),
		},
	}
}

// stubAttributes returns a new map where every attribute is Optional+Computed,
// has no validators, and has no plan modifiers — making it a silent no-op.
func stubAttributes(in map[string]schema.Attribute) map[string]schema.Attribute {
	if len(in) == 0 {
		return in
	}
	out := make(map[string]schema.Attribute, len(in))
	for k, a := range in {
		switch orig := a.(type) {
		case schema.StringAttribute:
			out[k] = schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: orig.Description,
			}
		case schema.BoolAttribute:
			out[k] = schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Description: orig.Description,
			}
		case schema.Int64Attribute:
			out[k] = schema.Int64Attribute{
				Optional:    true,
				Computed:    true,
				Description: orig.Description,
			}
		case schema.MapAttribute:
			out[k] = schema.MapAttribute{
				Optional:    true,
				Computed:    true,
				ElementType: orig.ElementType,
				Description: orig.Description,
			}
		default:
			// Fallback: keep as-is (should not occur with current schema).
			out[k] = a
		}
	}
	return out
}

// stubBlocks recursively stubs all blocks in the map.
func stubBlocks(in map[string]schema.Block) map[string]schema.Block {
	if len(in) == 0 {
		return in
	}
	out := make(map[string]schema.Block, len(in))
	for k, b := range in {
		switch orig := b.(type) {
		case schema.SingleNestedBlock:
			out[k] = stubSingleBlock(orig)
		case schema.ListNestedBlock:
			out[k] = stubListBlock(orig)
		default:
			out[k] = b
		}
	}
	return out
}
