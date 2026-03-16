terraform {
  required_providers {
    openlineage = {
      source  = "registry.terraform.io/tomasznazarewicz/openlineage-dataplex"
      version = "~> 0.1"
    }
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Minimal configuration
# Uses Application Default Credentials (ADC).
# Run: gcloud auth application-default login
# ──────────────────────────────────────────────────────────────────────────────
provider "openlineage" {
  project_id = "my-gcp-project"
  region     = "us-central1"
}

# ──────────────────────────────────────────────────────────────────────────────
# With explicit service-account key file
# ──────────────────────────────────────────────────────────────────────────────
# provider "openlineage" {
#   project_id       = "my-gcp-project"
#   region           = "us-central1"
#   credentials_file = "/path/to/service-account.json"
# }

# ──────────────────────────────────────────────────────────────────────────────
# With warn_on_unused_facets disabled (useful when migrating from another
# OpenLineage consumer — suppresses warnings about unsupported facets while
# you clean up config incrementally)
# ──────────────────────────────────────────────────────────────────────────────
# provider "openlineage" {
#   project_id          = "my-gcp-project"
#   region              = "us-central1"
#   warn_on_unused_facets = false
# }
