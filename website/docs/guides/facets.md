---
sidebar_position: 5
---

# Understanding and Using Facets

#### Adapted from the OpenLineage [spec](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md).

Facets are pieces of metadata that can be attached to the core entities of the spec:
- Run
- Job
- Dataset (Inputs or Outputs)

A facet is an atomic piece of metadata identified by its name. This means that emitting a new facet with the same name for the same entity replaces the previous facet instance for that entity entirely. It is defined as a JSON object that can be either part of the spec or a custom facet defined in a different project.

Custom facets must use a distinct prefix named after the project defining them to avoid collision with standard facets defined in the [OpenLineage.json](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json) spec.
They have a `\_schemaURL` field pointing to the corresponding version of the facet schema (as a JSONPointer: [$ref URL location](https://swagger.io/docs/specification/using-ref/) ).

For example: https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/MyCustomJobFacet

The versioned URL must be an immutable pointer to the version of the facet schema. For example, it should include a tag of a git sha and not a branch name. This should also be a canonical URL. There should be only one URL used for a given version of a schema.

Custom facets can be promoted to the standard by including them in the spec.

#### Custom Facet Naming

The naming of custom facets should follow the pattern `{prefix}{name}{entity}Facet` PascalCased.  
The prefix must be a distinct identifier named after the project defining it to avoid colision with standard facets defined in the [OpenLineage.json](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json) spec.
The entity is the core entity for which the facet is attached.

When attached to the core entity, the key should follow the pattern `{prefix}_{name}`, where both prefix and name follow snakeCase pattern. 

An example of a valid name is `BigQueryStatisticsJobFacet` and its key `bigQuery_statistics`.

### Standard Facets

#### Run Facets

- **nominalTime**: Captures the time this run is scheduled for. This is a typical usage for time based scheduled job. The job has a nominal schedule time that will be different from the actual time it is running at.

- **parent**: Captures the parent job and Run when the run was spawn from a parent run. For example in the case of Airflow, there's a run for the DAG that then spawns runs for individual tasks that would refer to the parent run as the DAG run. Similarly when a SparkOperator starts a Spark job, this creates a separate run that refers to the task run as its parent.

- **errorMessage**: Captures potential error message, programming language - and optionally stack trace - with which the run failed. 

#### Job Facets

- **sourceCodeLocation**: Captures the source code location and version (e.g., the git sha) of the job.

- **sourceCode**: Captures the language (e.g., Python) and actual source code of the job.

- **sql**: Capture the SQL query if this job is a SQL query.

- **ownership**: Captures the owners of the job.

#### Dataset Facets

- **schema**: Captures the schema of the dataset.

- **dataSource**: Captures the database instance containing this dataset (e.g., Database schema, Object store bucket, etc.)

- **lifecycleStateChange**: Captures the lifecycle states of the dataset (e.g., alter, create, drop, overwrite, rename, truncate).

- **version**: Captures the dataset version when versioning is defined by database (e.g., Iceberg snapshot ID).

- [**columnLineage**](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ColumnLineageDatasetFacet.json): Captures the column-level lineage.

- **ownership**: Captures the owners of the dataset.

#### Input Dataset Facets

- **dataQualityMetrics**: Captures dataset-level and column-level data quality metrics when scanning a dataset whith a DataQuality library (row count, byte size, null count, distinct count, average, min, max, quantiles).

- **dataQualityAssertions**: Captures the result of running data tests on a dataset or its columns.

#### Output Dataset Facets
- **outputStatistics**: Captures the size of the output written to a dataset (row count and byte size).

