# OpenLineage Spec

## Specification

The specification for OpenLineage is formalized as an OpenAPI spec: [OpenLineage.yml](OpenLineage.yml)
published at: https://openlineage.github.io/
It allows extensions to the spec using `Custom Facets` as described in this document.

## Core concepts

### Core Lineage Model

![Open Lineage model](OpenLineageModel.svg)

- **Run State Update**: and event describing an observed state of a job run. It is required to at least send one event for a START transition and a COMPLETE/FAIL/ABORT transition. Aditional events are optional.

- **Job**: a process definition that consumes and produces datasets (defined as its inputs and outputs). It is identified by a unique name within a namespace (which is typicaly assigned to the scheduler starting the jobs). The *Job* evolves over time and this change is captured when the job runs.

- **Dataset**: an abstract representation of data. It has a unique name within a namespace derived from its physical location (for example db.host.database.schema.table). Typicaly, a *Dataset* changes when a job writing to it completes.

- **Run**: An instance of a running job with a start and completion (or failure) time. It is uniquely identified by an id relative to its job definition.

- **Facet**: A piece of metadata attached to one of the entities defined above.

example:
Here is an example of a simple start run event not adding any facet information:
```
{
  "transition": "START",
  "eventTime": "2020-12-09T23:37:31.081Z",
  "run": {
    "runId": "345",
  },
  "job": {
    "namespace": "my-scheduler-namespace",
    "name": "myjob.mytask",
  },
  "inputs": [
    {
      "namespace": "my-datasource-namespace",
      "name": "instance.schema.table",
    }
  ],
  "outputs": [
    {
      "namespace": "my-datasource-namespace",
      "name": "instance.schema.output_table",
    }
  ],
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```

### Lifecycle

The OpenLineage API defines events to capture the lifecycle of a *Run* for a given *Job*.
When a *job* is being *run*, we capture metadata by sending run events when the state of the job transitions to a different state.
We might observe different aspects of the job run at different stages. This means that different metadata might be collected in each event during the lyfecycle of a run.
All metadata is additive. for example, if more inputs or outputs are detected as the job is running we might send additional events specifically for those datasets without re-emiting previously observed inputs or outputs.
Example:
 - When the run starts, we collect the following Metadata:
    - Run Id
    - Job id
    - transition: START
    - event time
    - source location and version (ex: git sha)
    - If known: Job inputs and outputs. (input schema, ...)
 - When the run completes:
    - Run Id
    - Job id
    - transition: COMPLETE
    - event time
    - Output datasets schema (and other metadata).



### Facets

Facets are pieces of metadata that can be attached to the core entities:
- Run
- Job
- Dataset

A facet is an atomic piece of metadata identified by its name. This means that emiting a new facet whith the same name for the same entity replaces the previous facet instance for that entity entirely). It is defined as a JSON object that can be either part of the spec or custom facets defined in a different project.

Custom facets must use a distinct prefix named after the project defining them to avoid colision with standard facets defined in the [OpenLineage.yml](OpenLineage.yml) OpenAPI spec.
They have a schemaURL field pointing to the corresponding version of the facet schema (as a [$ref URL location](https://swagger.io/docs/specification/using-ref/) ).

Example: https://github.com/OpenLineage/OpenLineage/blob/v1/spec/OpenLineage.yml#MyCustomJobFacet

The versioned URL must be an immutable pointer to the version of the facet schema. For example, it should include a tag of a git sha and not a branch name. This should also be a canonical URL. There should be only one URL used for a given version of a schema.

Custom facets can be promoted to the standard by including them in the spec.

### Standard Facets

#### Run Facets

- **nominalTime**: Captures the time this run is scheduled for. This is a typical usage for time based scheduled job. The job has a nominal schedule time that will be different from the actual time it is running at.

- **parent**: Captures the parent job and Run when the run was spawn from a parent run. For example in the case of Airflow, there's a run for the DAG that then spawns runs for individual tasks that would refer to the parent run as the DAG run. Similarly when a SparkOperator starts a Spark job, this creates a separate run that refers to the task run as its parent.

#### Job Facets

- **sourceCodeLocation**: Captures the source code location and version (example: git sha) of the job.

- **sql**: Capture the SQL query if this job is a SQL query.

#### Dataset Facets

- **schema**: Captures the schema of the dataset

- **dataSource**: Captures the Database instance containing this datasets (ex: Database schema. Object store bucket, ...)
