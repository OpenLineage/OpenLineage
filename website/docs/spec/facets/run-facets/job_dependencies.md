---
sidebar_position: 5
---

# Job Dependencies Facet

Modern data workflows often involve jobs that depend on the completion of other jobs before they can run. 
While tools like Apache Airflow track these dependencies internally, understanding them across systems is 
essential for full lineage visibility.

The ``JobDependenciesRunFacet`` captures direct upstream and downstream relationships between job runs, 
allowing OpenLineage to represent how execution order flows through a pipeline. It records which job runs 
must finish before the current run begins, and which job runs are waiting on this one to complete. It can 
also specify the trigger rule — for example, whether all upstream jobs must succeed before this one starts, or only one.

By tracking these dependencies across systems, OpenLineage can reconstruct cross-platform execution chains and provide 
clearer understanding of workflow orchestration and execution behavior.

Example: 

```json
{
  ...
"run": {
  "facets": {
    "jobDependencies": {
      "upstream": [
        {
          "job": {
            "name": "data-extract",
            "namespace": "pipeline.ingest"
          }
        },
        {
          "job": {
            "name": "user-profile-transform",
            "namespace": "pipeline.transform"
          },
          "run": {
            "runId": "6e9c2bb0-97d9-4d4f-9c0c-0579f072e013"
          }
        },
        {
          "job": {
            "name": "orders-cleanup",
            "namespace": "pipeline.preprocessing"
          },
          "run": {
            "runId": "bfc2d9b6-891a-4eee-8ef4-a45891b7c9fd"
          },
          "root": {
            "job": {
              "name": "daily-batch",
              "namespace": "orchestrator.dag"
            },
            "run": {
              "runId": "2bfdd1dc-79c7-43d5-8c5e-e0ea31bb48c9"
            }
          }
        }
      ],
      "downstream": [
        {
          "job": {
            "name": "analytics-warehouse-load",
            "namespace": "pipeline.load"
          },
          "run": {
            "runId": "a2ac0b8b-459c-44d0-b7d2-db6109ef5768"
          },
          "root": {
            "job": {
              "name": "daily-batch",
              "namespace": "orchestrator.dag"
            },
            "run": {
              "runId": "2bfdd1dc-79c7-43d5-8c5e-e0ea31bb48c9"
            }
          }
        },
        {
          "job": {
            "name": "dashboard-refresh",
            "namespace": "pipeline.analytics"
          },
          "run": {
            "runId": "7070ca59-60e0-4dbe-a1f5-4ee0c3a3195c"
          }
        },
        {
          "job": {
            "name": "email-send",
            "namespace": "pipeline.notifications"
          }
        }
      ],
      "trigger_rule": "NONE_FAILED_MIN_ONE_SUCCESS"
    }
  }
}
  ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/JobDependenciesRunFacet.json).