# Schema fields nested three levels deep (struct inside struct).
resource "openlineage_dataset" "test" {
  namespace = "s3://warehouse"
  name      = "nested"

  schema {
    fields {
      name = "address"
      type = "STRUCT"
      fields {
        name = "geo"
        type = "STRUCT"
        fields {
          name = "lat"
          type = "DOUBLE"
        }
      }
    }
  }
}
