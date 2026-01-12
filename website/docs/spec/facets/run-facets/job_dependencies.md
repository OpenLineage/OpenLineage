---
sidebar_position: 5
---

# Job Dependencies Facet

Modern data workflows often involve jobs that depend on the completion of other jobs before they can run. 
While tools like Apache Airflow track these dependencies internally, understanding them across systems is 
essential for full lineage visibility.

The ``JobDependenciesRunFacet`` captures execution dependencies (control flow relationships) between job runs, 
allowing OpenLineage to represent how execution order flows through a pipeline. It records which job runs 
must finish before the current run begins, and which job runs are waiting on this one to complete. The facet can also specify: 
- whether the upstream job needs to finish before the downstream job can be run 
- whether the upstream job needs to succeed/fail in order for the downstream job to be run 
- whether all upstream job dependencies must be met before the downstream job starts, or only one.

By tracking these dependencies across systems, OpenLineage can reconstruct cross-platform execution chains and provide 
clearer understanding of workflow orchestration and execution behavior.

Job execution dependencies documented using this facet should be interpreted as control flow relationships, they do not imply whether 
there is a data lineage relationship between the jobs (e.g. whether upstream job transforms data and passes them over to the downstream job). To represent job-to-job data lineage relationships follow [this example](https://openlineage.io/docs/development/examples#job-to-job-lineage---etl-job-with-several-tasks).

Example: 

```json
{
  ...
"run": {
  "facets": {
    "jobDependencies": {
      "upstream": [
        {
          "type": "IMPLICIT_DEPENDENCY",
          "sequence_trigger_rule": "FINISH_TO_START",
          "status_trigger_rule": "EXECUTE_ON_SUCCESS",
          "job": {
            "name": "data-extract",
            "namespace": "pipeline.ingest"
          }
        },
        {
          "type": "IMPLICIT_DEPENDENCY",
          "sequence_trigger_rule": "FINISH_TO_START",
          "status_trigger_rule": "EXECUTE_ON_SUCCESS",
          "job": {
            "name": "user-profile-transform",
            "namespace": "pipeline.transform"
          },
          "run": {
            "runId": "6e9c2bb0-97d9-4d4f-9c0c-0579f072e013"
          }
        },
        {
          "type": "IMPLICIT_DEPENDENCY",
          "sequence_trigger_rule": "FINISH_TO_START",
          "status_trigger_rule": "EXECUTE_EVERY_TIME",
          "job": {
            "name": "orders-cleanup",
            "namespace": "pipeline.preprocessing"
          },
          "run": {
            "runId": "bfc2d9b6-891a-4eee-8ef4-a45891b7c9fd"
          }
        }
      ],
      "downstream": [
        {
          "type": "DIRECT_INVOCATION",
          "sequence_trigger_rule": "FINISH_TO_START",
          "status_trigger_rule": "EXECUTE_ON_SUCCESS",
          "job": {
            "name": "analytics-warehouse-load",
            "namespace": "pipeline.load"
          },
          "run": {
            "runId": "a2ac0b8b-459c-44d0-b7d2-db6109ef5768"
          }
        },
        {
          "type": "DIRECT_INVOCATION",
          "sequence_trigger_rule": "FINISH_TO_START",
          "status_trigger_rule": "EXECUTE_ON_SUCCESS",
          "job": {
            "name": "dashboard-refresh",
            "namespace": "pipeline.analytics"
          },
          "run": {
            "runId": "7070ca59-60e0-4dbe-a1f5-4ee0c3a3195c"
          },
          "airflow": {
              "dagrun_id": "some_dagrun_id",
              "another_important_info": "123"
          }
        },
        {
          "type": "DIRECT_INVOCATION",
          "sequence_trigger_rule": "FINISH_TO_START",
          "status_trigger_rule": "EXECUTE_EVERY_TIME",
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

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-1/JobDependenciesRunFacet.json).
