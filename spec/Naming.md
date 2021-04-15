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

## Schedulers
### Airflow

Naming hierarchy:
 * job => workspace/DAG
 * Task => unique within the job

Identifier :
 * Namespace: {scheduler namespace}
 * Unique name: {job}.{task}
