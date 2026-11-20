terraform {
  required_providers {
    openlineage = {
      source = "openlineage/openlineage"
    }
  }
}

provider "openlineage" {
  output_dir = "./out"
  # producer       = "https://example.com/my-pipeline"
  # job_event_type = "run" # wrap job events in COMPLETE RunEvents
}
