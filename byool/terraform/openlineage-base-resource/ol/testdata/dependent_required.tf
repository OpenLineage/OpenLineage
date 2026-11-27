# Lineage job input with a name but no namespace: the spec's dependentRequired needs both.
resource "openlineage_job" "test" {
  namespace = "tf-test"
  name      = "dependent-required"

  lineage {
    entries {
      job {
        inputs {
          job {
            name = "upstream"
          }
        }
      }
    }
  }
}
