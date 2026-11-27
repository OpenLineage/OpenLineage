resource "openlineage_job" "test" {
  namespace = "tf-test"
  name      = "union-none"

  lineage {
    entries {
    }
  }
}
