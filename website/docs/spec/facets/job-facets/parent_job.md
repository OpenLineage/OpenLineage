---
sidebar_position: 9
---

# Parent Job Facet

The ParentJobFacet documents the hierarchical relationship between a job and its parent job (and, optionally, its root job) from the perspective of how the jobs are **designed**. For example, an Airflow task is defined as a part of an Airflow DAG, a dbt model is defined as a part of a dbt project, and a Spark job might be defined as a part of a larger pipeline.
This relationship exists regardless of whether the parent job ever runs.

It is the design-time counterpart of the [Parent Run Facet](../run-facets/parent_run.md). The Parent Run Facet primarily captures runtime information: which run spawned the current run. The Parent Job Facet carries the same `job` and `root` information, but it intentionally has no `run` objects and no run IDs, because a job definition is not tied to any particular run.

## When to use it

The Parent Job Facet should be used **only on Job events**.

On Run events the [Parent Run Facet](../run-facets/parent_run.md) takes precedence. If a producer emits a Run event for a job, it should describe the hierarchy using the Parent Run Facet, and consumers should prefer it over the Parent Job Facet if both are present.

## Structure

* `job` (required) - the parent job, identified by `namespace` and `name`. It can optionally carry a `facets` field with a
  selected subset of the parent job's facets, forwarded for convenience.
* `root` (optional) - the root of the whole design hierarchy, for example the Airflow DAG that eventually contains a task which spawns a Spark job. It contains a `job` object (`namespace`, `name` and optional `facets`).

As with the Parent Run Facet, the forwarded `facets` are not designed to replace the information contained in the parent job's own event, and should not be treated as a source of truth if the two ever differ.

## Example

```json
{
  ...
  "job": {
    "namespace": "airflow",
    "name": "my_dag.my_task",
    "facets": {
      "parent": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json",
        "job": {
          "namespace": "airflow",
          "name": "my_dag",
          "facets": {
            "jobType": {
              "processingType": "BATCH",
              "integration": "AIRFLOW",
              "jobType": "DAG"
            }
          }
        },
        "root": {
          "job": {
            "namespace": "airflow",
            "name": "my_dag"
          }
        }
      }
    }
  }
  ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json).
