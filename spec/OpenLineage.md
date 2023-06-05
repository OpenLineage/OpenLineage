# OpenLineage Spec

## Specification

The specification for OpenLineage is formalized as a JsonSchema [OpenLineage.json](OpenLineage.json). An OpenAPI spec is
also provided for HTTP-based implementations: [OpenLineage.yml](OpenLineage.yml) The documentation is published at:
https://openlineage.github.io/ It allows extensions to the spec using `Custom Facets` as described in this document.

## Core concepts

### Core Lineage Model

![Open Lineage model](OpenLineageModel.svg)

- **Run Event**: an event describing an observed state of a job run. Sending at least a START event and a
  COMPLETE/FAIL/ABORT event is required. Additional events are optional.

- **Job**: a process definition that consumes and produces datasets (defined as its inputs and outputs). It is
  [identified by a unique name within a namespace](Naming.md#Jobs) (which is assigned to the scheduler starting the
  jobs). The _Job_ evolves over time, and this change is captured when the job runs.

- **Dataset**: an abstract representation of data. It has a
  [unique name within the datasource namespace](Naming.md#Datasets) derived from its physical location
  (db.host.database.schema.table, for example). Typically, a _Dataset_ changes when a job writing to it completes.
  Similarly to the _Job_ and _Run_ distinction, metadata that is more static from run to run is captured in a
  DatasetFacet (for example, the schema that does not change every run), but what changes every _Run_ is captured as an
  _InputFacet_ or an _OutputFacet_ (for example, what subset of the data set was read or written, such as a time
  partition).

- **Run**: An instance of a running job with a start and completion (or failure) time. A run is identified by a globally
  unique ID relative to its job definition. A run ID **must** be a
  [UUID](https://datatracker.ietf.org/doc/html/rfc4122).

- **Facet**: A piece of metadata attached to one of the entities defined above.

Example: Here is an example of a simple start run event not adding any facet information:

```
{
  "eventType": "START",
  "eventTime": "2020-12-09T23:37:31.081Z",
  "run": {
    "runId": "3b452093-782c-4ef2-9c0c-aafe2aa6f34d",
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
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
  "schemaURL": "https://openlineage.io/spec/1-0-0/OpenLineage.json#/definitions/RunEvent"
}
```

### Lifecycle

The OpenLineage API defines events to capture the lifecycle of a _Run_ for a given _Job_. When a _job_ is being _run_,
we capture metadata by sending run events when the state of the job transitions to a different state. We might observe
different aspects of the job run at different stages. This means that different metadata might be collected in each
event during the lifecycle of a run. All metadata is additive. For example, if more inputs or outputs are detected as
the job is running, we might send additional events specifically for those datasets without re-emitting previously
observed inputs or outputs. Example:

- When the run starts, we collect the following Metadata:
  - Run ID
  - Job ID
  - eventType: START
  - event time
  - source location and version (e.g., git sha)
  - If known: Job inputs and outputs (input schema, etc.)
- When the run completes:
  - Run ID
  - Job ID
  - eventType: COMPLETE
  - event time
  - Output datasets schema (and other metadata).

### Facets

Facets are pieces of metadata that can be attached to the core entities:

- Run
- Job
- Dataset (Inputs or Outputs)

A facet is an atomic piece of metadata identified by its name. This means that emitting a new facet with the same name
for the same entity replaces the previous facet instance for that entity entirely. It is defined as a JSON object that
can be either part of the spec or custom facets defined in a different project.

Custom facets must use a distinct prefix named after the project defining them to avoid collision with standard facets
defined in the [OpenLineage.json](OpenLineage.json) spec. They have a \_schemaURL field pointing to the corresponding
version of the facet schema (as a JSONPointer: [$ref URL location](https://swagger.io/docs/specification/using-ref/) ).

Example:
https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/MyCustomJobFacet

The versioned URL must be an immutable pointer to the version of the facet schema. For example, it should include a tag
of a git sha and not a branch name. This should also be a canonical URL. There should be only one URL used for a given
version of a schema.

Custom facets can be promoted to the standard by including them in the spec.

#### Custom Facet Naming

Naming of custom facets should follow the pattern `{prefix}{name}{entity}Facet` PascalCased.  
The prefix must be a distinct identifier named after the project defining them to avoid colision with standard facets
defined in the [OpenLineage.json](OpenLineage.json) spec. The entity is the core entity for which the facet is attached.

When attached to a core entity, the key should follow the pattern `{prefix}_{name}`, where both prefix and name are in
snakeCase.

An example of a valid name is `BigQueryStatisticsJobFacet` and key is `bigQuery_statistics`.

### Standard Facets

#### Run Facets

- **nominalTime**: Captures the time this run is scheduled for. This is a typical usage for a time-based scheduled job.
  The job has a nominal schedule time that will be different from the actual time at which it is running.

- **parent**: Captures the parent job and run when the run has been spawned from a parent run. For example, in the case
  of Airflow, there is often a run for a DAG that then spawns runs for individual tasks that refer to the parent run as
  the DAG run. Similarly, when a SparkOperator starts a Spark job, this creates a separate run that refers to the task
  run as its parent.

- **errorMessage**: Captures the error message, programming language - and optionally stack trace - when a run fails.

#### Job Facets

- **sourceCodeLocation**: Captures the source code location and version (e.g., git sha) of the job.

- **sourceCode**: Captures the language (e.g., Python) and actual source code of the job.

- **sql**: Capture the SQL query if the job is a SQL query.

- **ownership**: Captures the owners of the job

#### Dataset Facets

- **schema**: Captures the schema of the dataset

- **dataSource**: Captures the Database instance containing the dataset (e.g., Database schema, Object store bucket,
  etc.)

- **lifecycleStateChange**: Captures the lifecycle states of the dataset (alter, create, drop, overwrite, rename,
  truncate, etc.).

- **version**: Captures the dataset version when versioning is defined by the database (e.g., Iceberg snapshot ID)

- [**columnLineage**](facets/ColumnLineageDatasetFacet.md): Captures the column-level lineage

- **ownership**: Captures the owners of the dataset

#### Input Dataset Facets

- **dataQualityMetrics**: Captures dataset-level and column-level data quality metrics when scanning a dataset whith a
  DataQuality library (row count, byte size, null count, distinct count, average, min, max, quantiles).

- **dataQualityAssertions**: Captures the result of running data tests on a dataset or its columns.

#### Output Dataset Facets

- **outputStatistics**: Captures the size of the output written to a dataset (row count and byte size).

---

SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project
