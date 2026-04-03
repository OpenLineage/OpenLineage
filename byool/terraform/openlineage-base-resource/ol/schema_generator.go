/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
)

// GenerateJobSchema builds the Terraform schema for an openlineage_job resource.
// Facets enabled in the JobCapability are included as normal (required/optional).
// Facets not enabled in the JobCapability are included as no-op stubs: every
// attribute inside them is Optional+Computed so a config copied from another
// consumer is accepted without error, but the values are silently ignored.
// Consumer-specific attributes (process_name etc.) are merged in by BaseJobResource.Schema().
func GenerateJobSchema(cap JobCapability) schema.Schema {
	blocks := map[string]schema.Block{}

	addJobBlock := func(f Facet, key string, full schema.Block) {
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

	// dataset identity is promoted to top-level (namespace + name are required)
	// the other dataset attributes come from datasetSchema
	identityAttrs := datasetIdentityAttributes()
	for k, v := range attrs {
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
		"description": schema.StringAttribute{
			Optional:    true,
			Description: "Job description",
		},
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
			"processing_type": schema.StringAttribute{
				Optional:    true,
				Description: "BATCH or STREAMING",
			},
			"integration": schema.StringAttribute{
				Optional:    true,
				Description: "Integration type e.g. SPARK, AIRFLOW, DBT, BYOL",
			},
			"job_type": schema.StringAttribute{
				Optional:    true,
				Description: "Job type e.g. QUERY, DAG, TASK, JOB, MODEL",
			},
		},
	}
}

func jobOwnershipBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Job owners (facets.OwnershipJobFacet)",
		Blocks: map[string]schema.Block{
			"owners": schema.ListNestedBlock{
				Description: "Owner entries",
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Required:    true,
							Description: "Owner identifier e.g. team:data-engineering",
						},
						"type": schema.StringAttribute{
							Required:    true,
							Description: "Owner type e.g. MAINTAINER, OWNER, STEWARD",
						},
					},
				},
			},
		},
	}
}

func jobDocumentationBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Human-readable documentation for this job (facets.DocumentationJobFacet)",
		Attributes: map[string]schema.Attribute{
			"description": schema.StringAttribute{
				Required:    true,
				Description: "Job documentation text",
			},
		},
	}
}

func sourceCodeBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Source code that implements this job (facets.SourceCode)",
		Attributes: map[string]schema.Attribute{
			"language": schema.StringAttribute{
				Required:    true,
				Description: "Programming language e.g. Python, Scala, SQL",
			},
			"source_code": schema.StringAttribute{
				Required:    true,
				Description: "The source code text or a URI pointing to it",
			},
		},
	}
}

func sourceCodeLocationBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "VCS location of the source code for this job (facets.SourceCodeLocation)",
		Attributes: map[string]schema.Attribute{
			"type": schema.StringAttribute{
				Required:    true,
				Description: "VCS type e.g. git",
			},
			"url": schema.StringAttribute{
				Required:    true,
				Description: "URL of the repository or file e.g. https://github.com/org/repo",
			},
			"repo_url": schema.StringAttribute{
				Optional:    true,
				Description: "Repository root URL when url points to a specific file",
			},
			"path": schema.StringAttribute{
				Optional:    true,
				Description: "Path within the repository",
			},
			"version": schema.StringAttribute{
				Optional:    true,
				Description: "Commit hash, tag, or branch name",
			},
			"tag": schema.StringAttribute{
				Optional:    true,
				Description: "VCS tag",
			},
			"branch": schema.StringAttribute{
				Optional:    true,
				Description: "Branch name",
			},
		},
	}
}

func sqlBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "SQL query executed by this job (facets.SQL)",
		Attributes: map[string]schema.Attribute{
			"query": schema.StringAttribute{
				Required:    true,
				Description: "The SQL query string",
			},
		},
	}
}

func jobTagsBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Free-form tags attached to this job (facets.TagsJobFacet)",
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"name": schema.StringAttribute{
					Required:    true,
					Description: "Tag name",
				},
				"value": schema.StringAttribute{
					Optional:    true,
					Description: "Tag value",
				},
				"description": schema.StringAttribute{
					Optional:    true,
					Description: "Tag description",
				},
			},
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
	if cap.IsEnabled(FacetDatasetColumnLineage) {
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
		"namespace": schema.StringAttribute{Required: true, Description: "Dataset namespace"},
		"name":      schema.StringAttribute{Required: true, Description: "Dataset name"},
	}
	blocks := map[string]schema.Block{}

	// Each facet is always included in the schema. When the facet is not enabled by
	// the consumer's capability, it is registered as a no-op stub (all attributes
	// Optional+Computed) so that configs shared between providers are accepted
	// without error — the values are simply ignored by the consumer.
	addBlock := func(f Facet, key string, full schema.Block) {
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
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"namespace": schema.StringAttribute{Required: true, Description: "Alternate namespace"},
				"name":      schema.StringAttribute{Required: true, Description: "Alternate name"},
				"type":      schema.StringAttribute{Required: true, Description: "e.g. TABLE, VIEW"},
			},
		},
	}
}

func datasetSchemaBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset schema / column definitions (facets.Schema)",
		Blocks: map[string]schema.Block{
			"fields": schema.ListNestedBlock{
				Description: "Column definitions",
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"name":        schema.StringAttribute{Required: true, Description: "Column name"},
						"type":        schema.StringAttribute{Optional: true, Description: "Data type e.g. VARCHAR, INT64"},
						"description": schema.StringAttribute{Optional: true, Description: "Column description"},
					},
				},
			},
		},
	}
}

func dataSourceBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Source system for this dataset (facets.DataSource)",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{Required: true, Description: "Source system name e.g. my-postgres"},
			"uri":  schema.StringAttribute{Required: true, Description: "Source system URI e.g. postgresql://host:5432/db"},
		},
	}
}

func datasetDocumentationBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Human-readable documentation for this dataset (facets.DocumentationDatasetFacet)",
		Attributes: map[string]schema.Attribute{
			"description": schema.StringAttribute{Required: true, Description: "Dataset documentation"},
		},
	}
}

func datasetTypeBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset type classification (facets.DatasetType)",
		Attributes: map[string]schema.Attribute{
			"dataset_type":  schema.StringAttribute{Required: true, Description: "e.g. TABLE, VIEW, STREAM"},
			"media_type":    schema.StringAttribute{Optional: true, Description: "e.g. application/json"},
			"storage_layer": schema.StringAttribute{Optional: true, Description: "e.g. bigquery, hive"},
		},
	}
}

func datasetVersionBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset version at the time of this run (facets.Version)",
		Attributes: map[string]schema.Attribute{
			"dataset_version": schema.StringAttribute{Required: true, Description: "Dataset version identifier"},
		},
	}
}

func storageBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Physical storage of this dataset (facets.Storage)",
		Attributes: map[string]schema.Attribute{
			"storage_layer": schema.StringAttribute{Required: true, Description: "e.g. iceberg, delta, hive"},
			"file_format":   schema.StringAttribute{Optional: true, Description: "e.g. parquet, orc"},
		},
	}
}

func datasetOwnershipBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset owners (facets.OwnershipDatasetFacet)",
		Blocks: map[string]schema.Block{
			"owners": schema.ListNestedBlock{
				Description: "Owner entries",
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{Required: true, Description: "Owner identifier"},
						"type": schema.StringAttribute{Optional: true, Description: "Owner type e.g. MAINTAINER"},
					},
				},
			},
		},
	}
}

func lifecycleStateChangeBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset lifecycle state transition (facets.LifecycleStateChange)",
		Attributes: map[string]schema.Attribute{
			"lifecycle_state_change": schema.StringAttribute{
				Required:    true,
				Description: "e.g. CREATE, DROP, ALTER, RENAME, OVERWRITE",
			},
		},
		Blocks: map[string]schema.Block{
			"previous_identifier": schema.SingleNestedBlock{
				Description: "Previous namespace+name before a RENAME",
				Attributes: map[string]schema.Attribute{
					"namespace": schema.StringAttribute{Required: true, Description: "Previous namespace"},
					"name":      schema.StringAttribute{Required: true, Description: "Previous name"},
				},
			},
		},
	}
}

func hierarchyBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Dataset position in a hierarchy e.g. partition within a table (facets.Hierarchy)",
		Blocks: map[string]schema.Block{
			"parent":   hierarchyElementBlock("Parent dataset in the hierarchy"),
			"children": hierarchyElementListBlock("Child datasets in the hierarchy"),
		},
	}
}

func hierarchyElementBlock(desc string) schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: desc,
		Attributes: map[string]schema.Attribute{
			"namespace": schema.StringAttribute{Required: true, Description: "Namespace"},
			"name":      schema.StringAttribute{Required: true, Description: "Name"},
			"type":      schema.StringAttribute{Optional: true, Description: "e.g. TABLE, PARTITION"},
		},
	}
}

func hierarchyElementListBlock(desc string) schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: desc,
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"namespace": schema.StringAttribute{Required: true, Description: "Namespace"},
				"name":      schema.StringAttribute{Required: true, Description: "Name"},
				"type":      schema.StringAttribute{Optional: true, Description: "e.g. TABLE, PARTITION"},
			},
		},
	}
}

func catalogBlock() schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: "Catalog/metastore registration (facets.Catalog)",
		Attributes: map[string]schema.Attribute{
			"framework":     schema.StringAttribute{Required: true, Description: "e.g. hive, iceberg"},
			"type":          schema.StringAttribute{Required: true, Description: "Catalog type e.g. hive"},
			"name":          schema.StringAttribute{Required: true, Description: "Catalog name"},
			"metadata_uri":  schema.StringAttribute{Optional: true, Description: "e.g. hive://localhost:9083"},
			"warehouse_uri": schema.StringAttribute{Optional: true, Description: "e.g. hdfs://localhost/warehouse"},
			"source":        schema.StringAttribute{Optional: true, Description: "Source system e.g. spark"},
		},
	}
}

func datasetTagsBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Free-form tags on this dataset (facets.TagsDatasetFacet)",
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"name":        schema.StringAttribute{Required: true, Description: "Tag name"},
				"value":       schema.StringAttribute{Optional: true, Description: "Tag value"},
				"description": schema.StringAttribute{Optional: true, Description: "Tag description"},
			},
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
	}
}

func columnLineageFieldsBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Field-level lineage: output column → input columns",
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"name": schema.StringAttribute{Required: true, Description: "Output column name"},
			},
			Blocks: map[string]schema.Block{
				"input_field": schema.ListNestedBlock{
					Description: "Input fields that contribute to this output column",
					NestedObject: schema.NestedBlockObject{
						Attributes: map[string]schema.Attribute{
							"namespace": schema.StringAttribute{Required: true, Description: "Input dataset namespace"},
							"name":      schema.StringAttribute{Required: true, Description: "Input dataset name"},
							"field":     schema.StringAttribute{Required: true, Description: "Input column name"},
						},
						Blocks: map[string]schema.Block{
							"transformation": transformationBlock(),
						},
					},
				},
			},
		},
	}
}

func columnLineageDatasetBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "Dataset-level lineage: input dataset → output field (column unknown)",
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"namespace": schema.StringAttribute{Required: true, Description: "Input dataset namespace"},
				"name":      schema.StringAttribute{Required: true, Description: "Input dataset name"},
				"field":     schema.StringAttribute{Required: true, Description: "Output field this dataset contributes to"},
			},
			Blocks: map[string]schema.Block{
				"transformation": transformationBlock(),
			},
		},
	}
}

func transformationBlock() schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: "How the input data was transformed to produce the output (facets.Transformation)",
		NestedObject: schema.NestedBlockObject{
			Attributes: map[string]schema.Attribute{
				"type":        schema.StringAttribute{Required: true, Description: "DIRECT or INDIRECT"},
				"subtype":     schema.StringAttribute{Optional: true, Description: "e.g. IDENTITY, AGGREGATION, FILTER"},
				"description": schema.StringAttribute{Optional: true, Description: "Human-readable transformation description"},
				"masking": schema.BoolAttribute{
					Optional:    true,
					Description: "True if this transformation masks/anonymises data",
					PlanModifiers: []planmodifier.Bool{
						boolplanmodifier.UseStateForUnknown(),
					},
				},
			},
		},
	}
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
