# Optional list blocks left out: column lineage input field without transformations,
# column lineage without the optional dataset-level dependencies.
resource "openlineage_job" "test" {
  namespace = "tf-test"
  name      = "optional-lists-omitted"

  outputs {
    namespace = "s3://warehouse"
    name      = "out"
    column_lineage {
      fields {
        name = "id"
        input_fields {
          namespace = "s3://raw"
          name      = "events"
          field     = "event_id"
        }
      }
    }
  }
}
