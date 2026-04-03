# ──────────────────────────────────────────────────────────────────────────────
# Minimal example — job identity, one input, one output.
# This is the smallest valid openlineage_job configuration.
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


resource "openlineage_job" "pipeline" {
  namespace   = "production"
  name        = "my-pipeline"
  description = "Customer data pipeline"

  job_type {
    processing_type = "BATCH"
    integration     = "AIRFLOW"
    job_type        = "DAG"
  }

  ownership {
    owners {
      name = "team:platform"
      type = "OWNER"
    }
  }

  inputs {
    namespace = "postgres"
    name      = "mydb.public.customers"

    schema {
      fields {
        name = "id"
        type = "INTEGER"
      }
      fields {
        name = "email"
        type = "VARCHAR"
      }
    }
  }

  outputs {
    namespace = "bigquery"
    name      = "my-project.analytics.customers_clean"
  }
}

output "job_file_path" {
  value = openlineage_job.pipeline.file_path
}
