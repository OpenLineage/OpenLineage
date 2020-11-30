# OpenLineage Spec

## Specification

The specification for OpenLineage is defined as an OpenAPI spec: [Openlineage.yml]
The details of that spec are explained bellow.

## Core concepts

### Core Lineage Model

**Job**: a process definition that consumes and produces datasets (defined as its inputs and outputs). It is identified by a unique name within a namespace (which is typicaly assigned to the scheduler starting the jobs). The *Job* evolves over time and this change is captured when the job runs.
**Dataset**: an abstract representation of data. It has a unique name derived from its physical location (for example db.host.database.schema.table). Typicaly, a *Dataset* changes when a job writing to it completes.
**Run**: An instance of a running job with a start and completion (or failure) time. It is uniquely identified by a uuid.
**Facet**: A piece of metadata attached to one of the entities defined above

example:
Here is an example of a simple start run event not adding any facet information:
{
 id: "1793529e-84f9-4740-9960-7b1f738bb2f0"
 transition: "start",
 transitionTime: "2020-11-30T00:01:23Z",
 job: {
    namespace: "scheduler-instance",
    name: "dag.task",
    inputs: [
        { dataset: { name: "db.instance.database.schema.table"}}
    ],
    outputs: [
        { dataset: { name: "db.instance.database.schema.myoutput"} }
    ]
  }
}

### Lifecycle

The OpenLineage API defines events to capture the lifecycle of a *Run* for a given *Job*.
When a *job* is being *run*, we capture metadata by sending run events when the state of the job transitions to a different state.
Example:
 - When the run starts, we collect the following Metadata:
    - Job name
    - source location and version (ex: git sha)
    - If known: Job inputs and outputs. (input schema, ...)
 - When the run completes:
    - Output datasets schema (and other metadata).

### Facets

Facets are pieces of metadata that can be attached to the core entities:
- Run
- Job
- Dataset

A facet is an atomic piece of metadata identified by its name. This means that emiting a new facet whith the same name for the same entity replaces the previous facet instance for that entity entirely). It is defined as a JSON object that can be either part of the spec or custom facets defined in a different project. Custom facets must use a distinct prefix named after the project defining them to avoid colision with standard facets defined in the [OpenLineage.yml] OpenAPI spec. Custom facets can be promoted to the standard by including them in the spec.


### Standard Facets

#### Run Facets
**NominalTime**: Captures the time this run is scheduled for. This is a typical usage for time based scheduled job. The job has a nominal schedule time that will be different from the actual time it is running at.

#### Job Facets
**SourceCodeLocation**: Captures the source code location and version (example: git sha) of the job.

#### Dataset Facets
**Schema**: Captures the schema of the dataset
**Datasource**: Captures the Database instance conatining this datasets (ex: Database schema. Object store bucket, ...)
