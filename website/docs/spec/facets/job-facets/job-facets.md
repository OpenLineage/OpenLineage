---
sidebar_position: 1
---

# Job Facets

Job Facets apply to a distinct instance of a job: an abstract `process` that consumes, executes, and produces datasets (defined as its inputs and outputs). It is identified by a `unique name` within a `namespace`. The *Job* evolves over time and this change is captured during the job runs. 

Use the [Lineage Job Facet](lineage.md) when a RunEvent or JobEvent needs to
describe exact dataset-to-dataset, job-to-dataset, dataset-to-job, or job-to-job
relationships instead of relying on lineage inferred from the event boundary.

Use the [Parent Job Facet](parent_job.md) on JobEvents to describe the design-time hierarchy between a job and its
parent job. On RunEvents, the [Parent Run Facet](../run-facets/parent_run.md) takes precedence.
