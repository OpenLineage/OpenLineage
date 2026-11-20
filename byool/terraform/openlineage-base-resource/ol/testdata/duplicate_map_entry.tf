resource "openlineage_job" "test" {
  namespace = "tf-test"
  name      = "duplicate-map-entry"

  outputs {
    namespace = "s3://warehouse"
    name      = "out"
    column_lineage {
      fields {
        name = "id"
        input_fields {
          namespace = "s3://raw"
          name      = "events"
          field     = "a"
        }
      }
      fields {
        name = "id"
        input_fields {
          namespace = "s3://raw"
          name      = "events"
          field     = "b"
        }
      }
    }
  }
}
