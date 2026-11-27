resource "openlineage_job" "test" {
  namespace = "tf-test"
  name      = "union-both"

  lineage {
    entries {
      job {
        name      = "a"
        namespace = "b"
      }
      dataset {
        name      = "c"
        namespace = "d"
      }
    }
  }
}
