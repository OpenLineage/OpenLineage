terraform {
  required_providers {
    openlineage = {
      source = "registry.terraform.io/openlineage/openlineage-file"
    }
  }
}

provider "openlineage" {
  output_dir   = "${path.module}/lineage"
  pretty_print = true
}
