# Naming

We define the unique name strategy per datasource to ensure it is followed uniformly independently from the type of job consuming and producing data.
The namespace for a dataset is the unique name for its datasource.
For jobs it is the scheduler.

## Datasets:
The namespace and name of a datasource can be combined to form a URI (scheme:[//authority]path)
* Namespace = scheme:[//authority] (the datasource)
* Name = path (the datasets)

### Data warehouses/data bases.
Datasets are called tables. Tables are organised in databases and schemas.
#### Postgres:
Datasource hierarchy:
 * Host
 * Port

Naming hierarchy:
 * Database
 * Schema
 * Table

Identifier:
 * Namespace: postgres://{host}:{port} of the service instance.
   * Scheme = postgres
   * Authority = {host}:{port}
 * Unique name: {database}.{schema}.{table}
   * URI =  postgres://{host}:{port}/{database}.{schema}.{table}
  
#### Snowflake
See: [Object Identifiers — Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/identifiers.html)

Datasource hierarchy:
 * account name

Naming hierarchy:
 * Database: {database name} => unique across the account
 * Schema: {schema name} => unique within the database
 * Table: {table name} => unique within the schema

Identifier:
 * Namespace: snowflake://{account name}
   * Scheme = snowflake
   * Authority = {account name}
 * Name: {database}.{schema}.{table}
   * URI =   snowflake://{account name}/{database}.{schema}.{table}

#### BigQuery
See: 
[Creating and managing projects | Resource Manager Documentation](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
[Introduction to datasets | BigQuery](https://cloud.google.com/bigquery/docs/datasets-intro)
[Introduction to tables | BigQuery](https://cloud.google.com/bigquery/docs/tables-intro)

Datasource hierarchy:
 * bigquery

Naming hierarchy:
 * Project Name: {project name} => is not unique
 * Project number: {project number} => numeric: is unique across google cloud
 * Project ID: {project id} => readable: is unique across google cloud
 * dataset: {dataset name} => is unique within a project
 * table: {table name} => is unique within a dataset

Identifier :
 * Namespace: bigquery
   * Scheme = bigquery
   * Authority = 
 * Unique name: {project id}.{dataset name}.{table name}
   * URI =   bigquery:{project id}.{schema}.{table}

### Distributed file systems/blob stores
#### GCS
Datasource hierarchy: none, naming is global

Naming hierarchy:
 * bucket name => globally unique
 * Path 

Identifier :
 * Namespace: gs://{bucket name}
   * Scheme = gs
   * Authority = {bucket name}
 * Unique name: {path}
   * URI =   gs://{bucket name}{path}

#### S3
Naming hierarchy:
 * bucket name => globally unique
 * Path 

Identifier :
 * Namespace: s3://{bucket name}
   * Scheme = s3
   * Authority = {bucket name}
 * Unique name: {path}
   * URI =   s3://{bucket name}{path}

#### HDFS
Naming hierarchy:
 * Namenode: host + port
 * Path

Identifier :
 * Namespace: hdfs://{namenode host}:{namenode port}
   * Scheme = hdfs
   * Authority = {namenode host}:{namenode port}
 * Unique name: {path}
   * URI =   hdfs://{namenode host}:{namenode port}{path}

## Jobs
### Context

A `Job` is a recurring data transformation with Inputs and outputs. Each instance is captured as a `Run` with corresponding metadata.
A `Run` event identifies the `Job` it is an instance of by providing the job’s unique identifier.
The `Job` identifier is composed of a `Namespace` and a `Name`. The job name is unique within that namespace.

The core property we want to identify about a `Job` is how it changes over time. Different schedules of the same logic applied to different datasets (possibly with different parameters) are different jobs. The notion of a `job` is tied to a recurring schedule with specific inputs and outputs. It could be an incremental update or a full reprocess or even a streaming job.
 
If the same code artifact (for example a spark jar or a templated SQL query) is used in the context of different schedules with different input or outputs then they are different jobs. 
We are interested first in how they affect the datasets they produce.

### Job Namespace and constructing job names

Jobs have a `name` that is unique to them in their `namespace` by construction. 

The Namespace is the root of the naming hierarchy. The job name is constructed to identify the job within that namespace.

Example:
 - Airflow:
   - Namespace: the namespace is assigned to the airflow instance. Ex: airflow-staging, airflow-prod
   - Job: each task in a DAG is a job. name: {dag name}.{task name}
 - Spark:
   - Namespace: If there’s a parent job, it uses its namespace otherwise the namespace is provided as a configuration as in airflow.
   - Spark app job name: the spark.app.name (this might not be unique)
   - Spark action job name: {spark.app.name}.{node.name}

### Parent job run: a nested hierarchy of Jobs

It is often the case that jobs are part of a nested hierarchy.
For example an Airflow DAG contains tasks. An instance of the DAG is finished when all of the tasks are finished. Similarly a Spark job can spawn multiple actions each of them running independently. Additionally, a Spark job can be launched by an Airflow task within a DAG.

Since what we care about is identifying the job as rooted in a recurring schedule, we want to capture that connection and make sure that we treat the same application logic triggered at different schedules as different jobs.

To capture this, a run event provides a `ParentRun` facet, referring to the parent `Job` and `Run`. This allows tracking a recurring job from the root of the schedule it is running for. 

https://github.com/OpenLineage/OpenLineage/blob/9445948e41b1880a349cb45ea31f2d6f65225ee0/spec/OpenLineage.json#L268-L317

Example:
{
  "run": {
    "runId": “uuid”
  }
  "job": {
    "namespace": “ns”
    "name": “name”
  }
}

