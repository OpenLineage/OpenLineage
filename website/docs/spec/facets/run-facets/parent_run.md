---
sidebar_position: 7
---

# Parent Run Facet

Commonly, scheduler systems like Apache Airflow will trigger processes on remote systems, such as on Apache Spark or Apache Beam jobs. 
Those systems might have their own OpenLineage integration and report their own job runs and dataset inputs/outputs. 
The ParentRunFacet allows those downstream jobs to report which jobs spawned them to preserve job hierarchy. 
To do that, the scheduler system should have a way to pass its own job and run id to the child job.

In addition to the information about the direct job that spawned the current job, contained in job and run fields, the ParentRunFacet
optionally contains information about the root job contained in the root field.
The root job represents the initial operation that started the whole chain of parent-child jobs - for example, the 
Airflow DAG execution that eventually spawned Airflow tasks which then spawned Spark jobs.

Example: 

```json
{
  ...
  "run": {
    "facets": {
      "parent": {
        "job": {
          "name": "the-execution-parent-job", 
          "namespace": "the-namespace"
        },
        "run": {
          "runId": "f99310b4-3c3c-1a1a-2b2b-c1b95c24ff11"
        },
        "root": {
          "job": {
            "name": "the-top-level-job",
            "namespace": "another-namespace"
          },
          "run": {
            "runId": "f1234567-4f4f-1a1a-2b2b-abcdef123456"
          }
        }
      }
    }
  }
  ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json).