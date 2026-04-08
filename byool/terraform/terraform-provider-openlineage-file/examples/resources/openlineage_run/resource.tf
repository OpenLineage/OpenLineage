# ──────────────────────────────────────────────────────────────────────────────
# Minimal example — job identity, one input, one output.
# This is the smallest valid openlineage_run configuration.
# ──────────────────────────────────────────────────────────────────────────────
resource "openlineage_run" "minimal" {
  namespace = "production"
  name      = "my-etl-job"

  inputs {
    namespace = "postgres"
    name      = "mydb.public.orders"
  }

  outputs {
    namespace = "bigquery"
    name      = "my-project.analytics.order_summary"
  }
}


# ──────────────────────────────────────────────────────────────────────────────
# Full example — job classification, ownership, input schema, and column-level
# lineage on the output. Demonstrates every facet enabled by this provider.
# ──────────────────────────────────────────────────────────────────────────────
resource "openlineage_run" "etl" {
  namespace   = "production"
  name        = "my-etl-job-full"
  description = "Nightly ETL from Postgres to BigQuery"

  job_type {
    processing_type = "BATCH"
    integration     = "SPARK"
    job_type        = "JOB"
  }

  ownership {
    owners {
      name = "team:data-engineering"
      type = "OWNER"
    }
  }

  inputs {
    namespace = "postgres"
    name      = "mydb.public.orders"

    schema {
      fields {
        name = "id"
        type = "INTEGER"
      }
      fields {
        name = "amount"
        type = "NUMERIC"
      }
    }
  }

  outputs {
    namespace = "bigquery"
    name      = "my-project.analytics.order_summary"

    column_lineage {
      fields {
        name = "total_amount"
        input_field {
          namespace = "postgres"
          name      = "mydb.public.orders"
          field     = "amount"
          transformation {
            type    = "DIRECT"
            subtype = "AGGREGATION"
            masking = false
          }
        }
      }
    }
  }
}

output "run_file_path" {
  value = openlineage_run.etl.file_path
}

output "run_emit_count" {
  value = openlineage_run.etl.emit_count
}
