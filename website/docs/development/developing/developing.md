---
sidebar_position: 1
---

# Developing With OpenLineage

As there are hundreds and possibly thousands databases, query engines and other tools you could use to process, create and move data, there's great chance that existing OpenLineage integrations won't cover your needs.

However, OpenLineage project also provides libraries you could use to write your own integration. 

### Clients

For [Python](../../client/python.md) and [Java](../../client/java/java.md), we've created clients that you can use to properly create and emit OpenLineage events to HTTP, Kafka, and other consumers.

### API Documentation

- [OpenAPI documentation](https://openlineage.io/apidocs/openapi/)
- [Java Doc](https://openlineage.io/apidocs/javadoc/)

### Common Library (Python)

Getting lineage from systems like BigQuery or Redshift isn't necessarily tied to orchestrator or processing engine you're using. For this reason, we've extracted
that functionality from our Airflow library and [packaged it for separate use](https://pypi.org/project/openlineage-integration-common/). 

### SQL parser

We've created a SQL parser that allows you to extract lineage from SQL statements. The parser is implemented in Rust; however, it's also available as a [Java](https://mvnrepository.com/artifact/io.openlineage/openlineage-sql-java) or [Python](https://pypi.org/project/openlineage-sql/) library.
You can take a look at its sourcecode on [GitHub](https://github.com/OpenLineage/OpenLineage/tree/main/integration/sql).

## Contributing

Before making any changes, please read [CONTRIBUTING](https://github.com/OpenLineage/OpenLineage/blob/main/CONTRIBUTING.md) first.

Thanks for your contributions to the project!
