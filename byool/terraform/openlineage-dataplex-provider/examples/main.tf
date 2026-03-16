terraform {
  required_providers {
    openlineage = {
      source = "registry.terraform.io/tomasznazarewicz/openlineage-dataplex"
    }
  }
}

# Set real values here, or use environment variables:
#   export GCP_PROJECT_ID=your-project
#   export GCP_REGION=us-central1
#   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
#
# If credentials_file is omitted, Application Default Credentials are used.
# To activate ADC: gcloud auth application-default login
provider "openlineage" {
  project_id       = "gcp-open-lineage-testing"
  region           = "us"
  # credentials_file = "" # Optional, if not set, Application Default Credentials will be used
}

resource "openlineage_job" "example" {
  namespace   = "example_job_namespace"
  name        = "example_job_name"
  description = "example job"

  inputs {
    namespace = "custom"
    name      = "example_input_name_1"

    symlinks {
      namespace = "symlink_namespace"
      name      = "symlink_name_input_1"
      type      = "TABLE"
    }

    catalog {
      name         = "example_catalog_name_1"
      framework    = "example_framework"
      type         = "example_catalog_type"
      metadata_uri  = "example://localhost:9083"
      warehouse_uri = "example://localhost/tmp/warehouse"
      source       = "example_source"
    }
  }

  inputs {
    namespace = "custom"
    name      = "example_input_name_2"

    symlinks {
      namespace = "symlink_namespace"
      name      = "symlink_name_input_2"
      type      = "TABLE"
    }

    catalog {
      name         = "example_input_name_2"
      framework    = "custom"
      type         = "example_catalog_type"
      metadata_uri  = "example://localhost:9083"
      warehouse_uri = "example://localhost/tmp/warehouse"
      source       = "example_source"
    }
  }

  outputs {
    namespace = "custom"
    name      = "example_output_name"

    symlinks {
      namespace = "symlink_namespace"
      name      = "symlink_name_output"
      type      = "TABLE"
    }

    catalog {
      name        = "example_output_name"
      framework    = "example_framework"
      type         = "example_catalog_type"
      metadata_uri  = "example://localhost:9083"
      warehouse_uri = "example://localhost/tmp/warehouse"
      source       = "example_source"
    }

    column_lineage {
      fields {
        name = "output_field_1"

        input_field {
          namespace = "example_namespace"
          name      = "example_input_name_1"
          field     = "input_field"

          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = ""
            masking     = false
          }
        }
      }

      fields {
        name = "output_field_2"

        input_field {
          namespace = "example_namespace"
          name      = "example_input_name_2"
          field     = "input_field"

          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = ""
            masking     = false
          }
        }
      }

      dataset {
        namespace = "example_namespace"
        name      = "example_input_name_1"
        field     = "dataset_input_field_1"

        transformation {
          type        = "INDIRECT"
          subtype     = "FILTER"
          description = ""
        }
      }
    }
  }
}

