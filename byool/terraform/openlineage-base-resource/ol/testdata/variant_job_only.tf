# A union element using only the job variant: the dataset variant block (whose name and
# namespace are required inside it) is absent and must not raise errors.
resource "openlineage_job" "test" {
  namespace = "tf-test"
  name      = "variant-job-only"

  lineage {
    entries {
      job {
        inputs {
          job {
            namespace = "airflow"
            name      = "upstream"
          }
        }
      }
    }
  }
}
