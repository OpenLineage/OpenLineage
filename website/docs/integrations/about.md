---
sidebar_position: 1
title: About
---

# OpenLineage Integrations

## Capability Matrix

:::caution
This matrix is not yet complete.
:::

The matrix below shows the relationship between an input facet and various mechanisms OpenLineage uses to gather metadata. Not all mechanisms collect data to fill in all facets, and some facets are specific to one integration.  
✔️: The mechanism does implement this facet.  
✖️: The mechanism does not implement this facet.  
An empty column means it is not yet documented if the mechanism implements this facet.

| Mechanism          | Integration       | Metadata Gathered                             | InputDatasetFacet | OutputDatasetFacet | SqlJobFacet | SchemaDatasetFacet | DataSourceDatasetFacet | DataQualityMetricsInputDatasetFacet | DataQualityAssertionsDatasetFacet | SourceCodeJobFacet | ExternalQueryRunFacet | DocumentationDatasetFacet | SourceCodeLocationJobFacet | DocumentationJobFacet | ParentRunFacet |
|:-------------------|:------------------|:----------------------------------------------|:------------------|:-------------------|:------------|:-------------------|:-----------------------|:------------------------------------|:----------------------------------|:-------------------|:----------------------|:--------------------------|:---------------------------|:----------------------|:---------------|
| dbt                | dbt Project Files | Lineage<br />Row count<br />Byte count.       | ✔️                 |                    |             |                    |                        |                                     |                                   |                    |                       |                           |                            |                       |                |
| Feast              | Feature Store Config | Lineage<br />Schema<br />Feature metadata  | ✔️                 | ✔️                  |             | ✔️                  | ✔️                      |                                     |                                   |                    |                       | ✔️                         |                            |                       |                |
| Great Expectations | Action            | Data quality assertions                       | ✔️                 |                    |             |                    |                        | ✔️                                   | ✔️                                 |                    |                       |                           |                            |                       |                |
| Spark              | SparkListener     | Schema<br />Row count<br /> Column lineage    | ✔️                 |                    |             |                    |                        |                                     |                                   |                    |                       |                           |                            |                       |                |


## Compatibility matrix

This matrix shows which data sources are known to work with each integration, along with the minimum versions required in the target system or framework.

| Platform	| Version	| Data Sources                                                                                                                                                                                                                         |
|:-------------------|:-------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Apache Spark | 2.4+ | JDBC<br />HDFS<br />Google Cloud Storage<br />Google BigQuery<br />BigTable<br />Spanner<br />CloudSQL<br />Google BigQuery<br />Google BigQuery<br />Amazon S3<br />Azure Blob Storage<br />Azure Data Lake Gen2<br />Azure Synapse |
| dbt | 0.20+ | Snowflake<br /> Google BigQuery                                                                                                                                                                                                      |
| Feast | 0.60+ | Feast-supported sources                                                                                              |

## Integration strategies

:::info
This section could use some more detail! You're welcome to contribute using the Edit link at the bottom.
:::

### Integrating with pipelines

![Integrating with Pipelines](integrate-pipelines.svg)

### Integrating with data sources

![Integrating with Data Sources](integrate-datasources.svg)

## Custom integrations

As there are hundreds and possibly thousands databases, query engines and other tools you could use to process, create and move data, there's great chance that existing OpenLineage integrations won't cover your needs.

However, OpenLineage project also provides libraries you could use to write your own integration. 

### Clients

For [Python](../client/python/python.md) and [Java](../client/java/java.md), we've created clients that you can use to properly create and emit OpenLineage events to HTTP, Kafka, and other consumers.


### Common Library (Python)

Getting lineage from systems like BigQuery or Redshift isn't necessarily tied to orchestrator or processing engine you're using. For this reason, we've extracted
that functionality and [packaged it for separate use](https://pypi.org/project/openlineage-integration-common/). 

### SQL parser

We've created a SQL parser that allows you to extract lineage from SQL statements. The parser is implemented in Rust; however, it's also available as a [Java](https://mvnrepository.com/artifact/io.openlineage/openlineage-sql-java) or [Python](https://pypi.org/project/openlineage-sql/) library.
You can take a look at its sourcecode on [GitHub](https://github.com/OpenLineage/OpenLineage/tree/main/integration/sql).

### API Documentation

- [OpenAPI documentation](https://openlineage.io/apidocs/openapi/)
- [Java Doc](https://openlineage.io/apidocs/javadoc/)
