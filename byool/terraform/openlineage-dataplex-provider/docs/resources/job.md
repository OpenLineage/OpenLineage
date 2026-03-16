---
page_title: "openlineage_job Resource - openlineage"
subcategory: ""
description: |-
  Declares a data pipeline job and emits an OpenLineage COMPLETE RunEvent to GCP Dataplex Lineage.
---

# openlineage_job (Resource)

Declares a data pipeline job and emits an OpenLineage `COMPLETE` RunEvent to the
GCP Dataplex Lineage API when created or updated.

Each resource corresponds to one **Dataplex Process**. Every `apply` that changes
the resource configuration emits a new **Run** under that Process.

## Behaviour

| Terraform action | What happens in Dataplex |
|---|---|
| `apply` (create) | New Process + Run created via `ProcessOpenLineageRunEvent` |
| `apply` (update inputs/outputs) | New Run emitted under the same Process |
| `apply` (change `name` or `namespace`) | Old Process deleted, new Process + Run created |
| `plan` / `refresh` | `GetProcess` called — if deleted externally, resource is marked for re-create |
| `destroy` | `DeleteProcess` called — Process and all Runs removed |

## Example Usage

### Minimal

```terraform
# ──────────────────────────────────────────────────────────────────────────────
# Minimal — just job identity, inputs, and an output
# ──────────────────────────────────────────────────────────────────────────────
resource "openlineage_job" "minimal" {
  namespace = "airflow"
  name      = "analytics.daily_revenue"

  inputs {
    namespace = "bigquery"
    name      = "my-project.raw.orders"
  }

  outputs {
    namespace = "bigquery"
    name      = "my-project.analytics.daily_revenue"
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Full — job classification, ownership, symlinks, catalog metadata,
#         and field-level + dataset-level column lineage
# ──────────────────────────────────────────────────────────────────────────────
resource "openlineage_job" "full" {
  namespace   = "airflow"
  name        = "analytics.aggregate_product_sales"
  description = "Joins products and order items to produce daily sales statistics"

  # Classifies the job for Dataplex discovery
  job_type {
    processing_type = "BATCH"
    integration     = "SPARK"
    job_type        = "JOB"
  }

  # Ownership — listed in Dataplex process metadata
  owners {
    name = "team:data-engineering"
    type = "MAINTAINER"
  }

  owners {
    name = "user:alice@example.com"
    type = "OWNER"
  }

  # ── Input: products table ──────────────────────────────────────────────────
  inputs {
    namespace = "bigquery"
    name      = "my-project.raw.products"

    # Alternate identifier — e.g. the same table known under a Hive namespace
    symlinks {
      namespace = "hive"
      name      = "default.products"
      type      = "TABLE"
    }

    # Catalog / metastore registration
    catalog {
      framework     = "hive"
      type          = "hive"
      metadata_uri  = "hive://metastore.internal:9083"
      warehouse_uri = "gs://my-project-warehouse/hive"
      source        = "spark"
    }
  }

  # ── Input: order_items table ───────────────────────────────────────────────
  inputs {
    namespace = "bigquery"
    name      = "my-project.raw.order_items"

    symlinks {
      namespace = "hive"
      name      = "default.order_items"
      type      = "TABLE"
    }

    catalog {
      framework     = "hive"
      type          = "hive"
      metadata_uri  = "hive://metastore.internal:9083"
      warehouse_uri = "gs://my-project-warehouse/hive"
      source        = "spark"
    }
  }

  # ── Output: product_sales table ────────────────────────────────────────────
  outputs {
    namespace = "bigquery"
    name      = "my-project.analytics.product_sales"

    symlinks {
      namespace = "hive"
      name      = "analytics.product_sales"
      type      = "TABLE"
    }

    catalog {
      framework     = "iceberg"
      type          = "hive"
      metadata_uri  = "hive://metastore.internal:9083"
      warehouse_uri = "gs://my-project-warehouse/iceberg"
      source        = "spark"
    }

    # Field-level lineage: maps each output column back to its source column(s)
    column_lineage {
      # output column "total_price" comes directly from "unit_price" in order_items
      fields {
        name = "total_price"

        input_field {
          namespace = "bigquery"
          name      = "my-project.raw.order_items"
          field     = "unit_price"

          transformation {
            type        = "DIRECT"
            subtype     = "AGGREGATION"
            description = "SUM of unit prices grouped by product"
            masking     = false
          }
        }
      }

      # output column "product_name" is a direct copy from products.name
      fields {
        name = "product_name"

        input_field {
          namespace = "bigquery"
          name      = "my-project.raw.products"
          field     = "name"

          transformation {
            type    = "DIRECT"
            subtype = "IDENTITY"
            masking = false
          }
        }
      }

      # output column "category" is derived indirectly — filtering applied
      fields {
        name = "category"

        input_field {
          namespace = "bigquery"
          name      = "my-project.raw.products"
          field     = "category"

          transformation {
            type        = "INDIRECT"
            subtype     = "FILTER"
            description = "Only active product categories included"
            masking     = false
          }
        }
      }

      # Dataset-level lineage: the entire order_items table contributes to
      # the "order_count" field but the exact column mapping is not tracked
      dataset {
        namespace = "bigquery"
        name      = "my-project.raw.order_items"
        field     = "order_count"

        transformation {
          type        = "INDIRECT"
          subtype     = "AGGREGATION"
          description = "COUNT of order rows per product"
        }
      }
    }
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Outputs — reference computed Dataplex resource names
# ──────────────────────────────────────────────────────────────────────────────
output "full_process_name" {
  description = "Dataplex Process resource name for the full example job"
  value       = openlineage_job.full.process_name
}

output "full_run_name" {
  description = "Dataplex Run resource name from the latest apply"
  value       = openlineage_job.full.run_name
}

output "full_lineage_event_name" {
  description = "Dataplex LineageEvent resource name from the latest apply"
  value       = openlineage_job.full.lineage_event_name
}
```

### Full — with symlinks, catalog, and column lineage

```hcl
resource "openlineage_job" "full_example" {
  namespace   = "airflow"
  name        = "analytics.aggregate_product_sales"
  description = "Joins products and order items to produce sales statistics"

  job_type {
    processing_type = "BATCH"
    integration     = "SPARK"
    job_type        = "JOB"
  }

  owners {
    name = "team:data-engineering"
    type = "MAINTAINER"
  }

  inputs {
    namespace = "bigquery"
    name      = "my-project.raw.products"

    symlinks {
      namespace = "hive"
      name      = "default.products"
      type      = "TABLE"
    }

    catalog {
      framework     = "hive"
      type          = "hive"
      metadata_uri  = "hive://localhost:9083"
      warehouse_uri = "hdfs://localhost/tmp/warehouse"
      source        = "spark"
    }
  }

  outputs {
    namespace = "bigquery"
    name      = "my-project.analytics.product_sales"

    column_lineage {
      fields {
        name = "total_price"

        input_field {
          namespace = "bigquery"
          name      = "my-project.raw.order_items"
          field     = "unit_price"

          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = "Sum of unit prices from order items"
            masking     = false
          }
        }
      }

      dataset {
        namespace = "bigquery"
        name      = "my-project.raw.order_items"
        field     = "product_id"

        transformation {
          type        = "INDIRECT"
          subtype     = "FILTER"
          description = "Filtered by active products"
        }
      }
    }
  }
}

output "process_name" {
  value = openlineage_job.full_example.process_name
}

output "run_state" {
  value = openlineage_job.full_example.run_state
}
```

<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `name` (String) Job name
- `namespace` (String) Job namespace

### Optional

- `description` (String) Job description
- `inputs` (Block List) Input datasets (see [below for nested schema](#nestedblock--inputs))
- `job_type` (Block, Optional) Job type classification (facets.JobType) (see [below for nested schema](#nestedblock--job_type))
- `outputs` (Block List) Output datasets (see [below for nested schema](#nestedblock--outputs))
- `owners` (Block List) Job owners (facets.OwnershipJobFacetOwnership) (see [below for nested schema](#nestedblock--owners))

### Read-Only

- `id` (String) Internal identifier (namespace.name)
- `lineage_event_name` (String) Dataplex lineage event resource name
- `process_name` (String) Dataplex process resource name
- `run_id` (String) UUID of the latest emitted run
- `run_name` (String) Dataplex run resource name
- `update_time` (String) Process last update time (ISO 8601)

<a id="nestedblock--inputs"></a>
### Nested Schema for `inputs`

Required:

- `name` (String) Dataset name
- `namespace` (String) Dataset namespace

Optional:

- `catalog` (Block, Optional) Catalog/metastore registration (facets.Catalog) (see [below for nested schema](#nestedblock--inputs--catalog))
- `symlinks` (Block List) Alternate dataset identifiers (facets.Symlinks) (see [below for nested schema](#nestedblock--inputs--symlinks))

<a id="nestedblock--inputs--catalog"></a>
### Nested Schema for `inputs.catalog`

Required:

- `framework` (String) e.g. hive, iceberg
- `name` (String) Catalog name
- `type` (String) Catalog type e.g. hive

Optional:

- `metadata_uri` (String) e.g. hive://localhost:9083
- `source` (String) Source system e.g. spark
- `warehouse_uri` (String) e.g. hdfs://localhost/warehouse


<a id="nestedblock--inputs--symlinks"></a>
### Nested Schema for `inputs.symlinks`

Required:

- `name` (String) Alternate name
- `namespace` (String) Alternate namespace
- `type` (String) e.g. TABLE, VIEW



<a id="nestedblock--job_type"></a>
### Nested Schema for `job_type`

Optional:

- `integration` (String) Integration type e.g. SPARK, AIRFLOW, DBT, BYOL
- `job_type` (String) Job type e.g. QUERY, DAG, TASK, JOB, MODEL
- `processing_type` (String) BATCH or STREAMING


<a id="nestedblock--outputs"></a>
### Nested Schema for `outputs`

Required:

- `name` (String) Dataset name
- `namespace` (String) Dataset namespace

Optional:

- `catalog` (Block, Optional) Catalog/metastore registration (facets.Catalog) (see [below for nested schema](#nestedblock--outputs--catalog))
- `column_lineage` (Block, Optional) Column-level lineage for this output dataset (facets.ColumnLineage) (see [below for nested schema](#nestedblock--outputs--column_lineage))
- `symlinks` (Block List) Alternate dataset identifiers (facets.Symlinks) (see [below for nested schema](#nestedblock--outputs--symlinks))

<a id="nestedblock--outputs--catalog"></a>
### Nested Schema for `outputs.catalog`

Required:

- `framework` (String) e.g. hive, iceberg
- `name` (String) Catalog name
- `type` (String) Catalog type e.g. hive

Optional:

- `metadata_uri` (String) e.g. hive://localhost:9083
- `source` (String) Source system e.g. spark
- `warehouse_uri` (String) e.g. hdfs://localhost/warehouse


<a id="nestedblock--outputs--column_lineage"></a>
### Nested Schema for `outputs.column_lineage`

Optional:

- `dataset` (Block List) Dataset-level lineage: input dataset → output field (column unknown) (see [below for nested schema](#nestedblock--outputs--column_lineage--dataset))
- `fields` (Block List) Field-level lineage: output column → input columns (see [below for nested schema](#nestedblock--outputs--column_lineage--fields))

<a id="nestedblock--outputs--column_lineage--dataset"></a>
### Nested Schema for `outputs.column_lineage.dataset`

Required:

- `field` (String) Output field this dataset contributes to
- `name` (String) Input dataset name
- `namespace` (String) Input dataset namespace

Optional:

- `transformation` (Block List) How the input data was transformed to produce the output (facets.Transformation) (see [below for nested schema](#nestedblock--outputs--column_lineage--dataset--transformation))

<a id="nestedblock--outputs--column_lineage--dataset--transformation"></a>
### Nested Schema for `outputs.column_lineage.dataset.transformation`

Required:

- `type` (String) DIRECT or INDIRECT

Optional:

- `description` (String) Human-readable transformation description
- `masking` (Boolean) True if this transformation masks/anonymises data
- `subtype` (String) e.g. IDENTITY, AGGREGATION, FILTER



<a id="nestedblock--outputs--column_lineage--fields"></a>
### Nested Schema for `outputs.column_lineage.fields`

Required:

- `name` (String) Output column name

Optional:

- `input_field` (Block List) Input fields that contribute to this output column (see [below for nested schema](#nestedblock--outputs--column_lineage--fields--input_field))

<a id="nestedblock--outputs--column_lineage--fields--input_field"></a>
### Nested Schema for `outputs.column_lineage.fields.input_field`

Required:

- `field` (String) Input column name
- `name` (String) Input dataset name
- `namespace` (String) Input dataset namespace

Optional:

- `transformation` (Block List) How the input data was transformed to produce the output (facets.Transformation) (see [below for nested schema](#nestedblock--outputs--column_lineage--fields--input_field--transformation))

<a id="nestedblock--outputs--column_lineage--fields--input_field--transformation"></a>
### Nested Schema for `outputs.column_lineage.fields.input_field.transformation`

Required:

- `type` (String) DIRECT or INDIRECT

Optional:

- `description` (String) Human-readable transformation description
- `masking` (Boolean) True if this transformation masks/anonymises data
- `subtype` (String) e.g. IDENTITY, AGGREGATION, FILTER





<a id="nestedblock--outputs--symlinks"></a>
### Nested Schema for `outputs.symlinks`

Required:

- `name` (String) Alternate name
- `namespace` (String) Alternate namespace
- `type` (String) e.g. TABLE, VIEW



<a id="nestedblock--owners"></a>
### Nested Schema for `owners`

Required:

- `name` (String) Owner identifier e.g. team:data-engineering
- `type` (String) Owner type e.g. MAINTAINER, OWNER, STEWARD




## Import

Existing Dataplex Processes can be imported using the Dataplex process resource name:

```shell
terraform import openlineage_job.example projects/my-project/locations/us-central1/processes/my-process-id
```
