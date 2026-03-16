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
