# OpenLineage Spec

## Specification

The specification for OpenLineage is defined as an OpenAPI spec: [Openlineage.yml]
The details of that spec are explained bellow.

## Core concepts

### Core Lineage Model

- **Job**: a process definition that consumes and produces datasets (defined as its inputs and outputs). It is identified by a unique name within a namespace (which is typicaly assigned to the scheduler starting the jobs). The *Job* evolves over time and this change is captured when the job runs.

- **Dataset**: an abstract representation of data. It has a unique name derived from its physical location (for example db.host.database.schema.table). Typicaly, a *Dataset* changes when a job writing to it completes.

- **Run**: An instance of a running job with a start and completion (or failure) time. It is uniquely identified by a uuid.
**Facet**: A piece of metadata attached to one of the entities defined above

example:
Here is an example of a simple start run event not adding any facet information:
```
{
  runId: "1793529e-84f9-4740-9960-7b1f738bb2f0"
  jobId: {
    namespace: "scheduler-instance",
    name: "dag.task"
  }
  transition: "start",
  transitionTime: "2020-11-30T00:01:23Z",
  origin: {
    name: "marquez-airflow",
    version: "1.2.3"
  }
  job: {
    description: "important job producing teh executive dashboard"
  }
  inputs: [
    { datasetId:{ namespace: "db", name: "db.instance.database.schema.table"} }
  ],
  outputs: [
    { datasetId: { namesapce: "db", name: "db.instance.database.schema.myoutput"} }
  ]
}
```

### Lifecycle

The OpenLineage API defines events to capture the lifecycle of a *Run* for a given *Job*.
When a *job* is being *run*, we capture metadata by sending run events when the state of the job transitions to a different state.
We might observe different aspects of the job run at different stages. This means that different metadata might be collected in each event during the lyfecycle of a run.
All metadata is additive. for example, if more inputs or outputs are detected as the job is running we might send updates specifically for those datasets without re-emiting previously observed inputs or outputs.
Example:
 - When the run starts, we collect the following Metadata:
    - Run Id
    - Job id
    - transition: START
    - transition time
    - source location and version (ex: git sha)
    - If known: Job inputs and outputs. (input schema, ...)
 - When the run completes:
    - Run Id
    - Job id
    - transition: COMPLETE
    - transition time
    - Output datasets schema (and other metadata).



### Facets

Facets are pieces of metadata that can be attached to the core entities:
- Run
- Job
- Dataset

A facet is an atomic piece of metadata identified by its name. This means that emiting a new facet whith the same name for the same entity replaces the previous facet instance for that entity entirely). It is defined as a JSON object that can be either part of the spec or custom facets defined in a different project. Custom facets must use a distinct prefix named after the project defining them to avoid colision with standard facets defined in the [OpenLineage.yml] OpenAPI spec. Custom facets can be promoted to the standard by including them in the spec.


### Standard Facets

#### Run Facets

- **nominalTime**: Captures the time this run is scheduled for. This is a typical usage for time based scheduled job. The job has a nominal schedule time that will be different from the actual time it is running at.

- **parent**: Captures the parent job and Run when the run was spawn from a parent run.

#### Job Facets

- **sourceCodeLocation**: Captures the source code location and version (example: git sha) of the job.

- **sql**: Capture the SQL query if this job is a SQL query.

#### Dataset Facets

- **schema**: Captures the schema of the dataset

- **dataSource**: Captures the Database instance containing this datasets (ex: Database schema. Object store bucket, ...)
