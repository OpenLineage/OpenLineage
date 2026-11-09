---
sidebar_position: 9
---

# Parent Job Facet

The ParentJobFacet documents the hierarchical relationship between a job and its parent job (and, optionally, its root job) from the perspective of how the jobs are **designed**. For example, an Airflow task is defined as a part of an Airflow DAG, a dbt model is defined as a part of a dbt project, and a Spark job might be defined as a part of a larger pipeline.
This relationship exists regardless of whether the parent job ever runs.

It is the design-time counterpart of the [Parent Run Facet](../run-facets/parent_run.md). The Parent Run Facet primarily captures runtime information: which run spawned the current run. The Parent Job Facet carries the same `job` and `root` information, but it intentionally has no `run` objects and no run IDs, because a job definition is not tied to any particular run.

## When to use it

Parent Job Facet is used to reference a design time relationship of a job to it's parent job. In a similar way, [Parent Run Facet](../run-facets/parent_run.md) is used to document a runtime invokation by a specific instance of a parent job. 

The primary use case for Parent Job facet is to document this design relationship within a Job event, but the facet can be used in the same way on a Run event in case the specific instance (runId) of the parent job is not known. If both facets are present on the same Run event, it is expected that they reference the same Job, but the in case there is conflict, Parent Run facet takes precedense. 


## Structure

* `job` (required) - the parent job, identified by `namespace` and `name`. It can optionally carry a `facets` field with a
  selected subset of the parent job's facets, forwarded for convenience.
* `root` (optional) - the root of the whole design hierarchy, for example the Airflow DAG that eventually contains a task which spawns a Spark job. It contains a `job` object (`namespace`, `name` and optional `facets`).

As with the Parent Run Facet, the forwarded `facets` are not designed to replace the information contained in the parent job's own event, and should not be treated as a source of truth if the two ever differ.

## Example

In this example, the event's job is a Spark job defined within the Airflow task `my_dag.my_task`, which is in turn defined within the DAG `my_dag`. The direct parent is the task and the root is the DAG.

```json
{
  ...
  "job": {
    "namespace": "spark",
    "name": "my_dag.my_task.my_spark_job",
    "facets": {
      "parent": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json",
        "job": {
          "namespace": "airflow",
          "name": "my_dag.my_task",
          "facets": {
            "jobType": {
              "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow",
              "_schemaURL": "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json",
              "processingType": "BATCH",
              "integration": "AIRFLOW",
              "jobType": "TASK"
            }
          }
        },
        "root": {
          "job": {
            "namespace": "airflow",
            "name": "my_dag",
            "facets": {
              "jobType": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json",
                "processingType": "BATCH",
                "integration": "AIRFLOW",
                "jobType": "DAG"
              }
            }
          }
        }
      }
    }
  }
  ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json).
