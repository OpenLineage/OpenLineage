resource "openlineage_dataset" "test" {
  namespace = "s3://warehouse"
  name      = "before"
}
