---
sidebar_position: 6
---

# OpenLineage for Spark Connectors

### What is OpenLineage
OpenLineage is an open standard for lineage data collection. It tracks metadata about core objects - datasets, jobs and runs - that represent how data is moving through the data pipelines. 

Besides describing standard events, OpenLineage project develops integration for popular open source data processing tools, like Apache Airflow, dbt, Apache Flink and Apache Spark, that allow users to automatically gather lineage metadata while the data jobs are running. 
How does Spark OpenLineage integration work? 
OpenLineage implements an instance of SparkListener interface, which allows it to listen to Spark events emitted during executions. Amongst those events are those that let us know that Spark Job has started or stopped running, like SparkListenerJobStart, SparkListenerJobEnd. When an OL listener receives that event, it can look up the LogicalPlan of a job, which represents a high level representation of a computation that Spark plans to do.

LogicalPlan has a tree-like structure. The leafs of the tree are sources of the data that describe where and how Spark is reading the input datasets. Then, data flows through intermediary nodes that describe some computation to be performed - like joins, or reshaping the data structure - like some projection. At the end, the root node describes where the data will end up. The peculiarity of that structure is that there is only one output node - if you write data to multiple output datasets, it’s represented as multiple jobs and LogicalPlan trees.

### What has OpenLineage to do with Spark connectors? 

LogicalPlan is an abstract class. The particular operations, whether reading data, processing it or writing it are implemented as a subclass of it, with attributes and methods allowing OL listener to interpret that data. OL Spark integration has a concept of visitors that receive nodes of the LogicalPlan - visitor defines the conditions - like, whether that LogicalPlan node is a particular subclass, like SaveIntoDataSourceCommand, or it’s received in particular phase of a Spark Job’s lifetime - and how to process data given it wants to do it. 

Spark Connectors, whether included by default in Spark or external to it, have few options on how to implement the necessary operations. This is a very simplified explanation.

First is to implement your own LogicalPlan nodes together with extending Spark Planner to make sure the right LogicalPlan is generated. This is the hardest route, and it’s how several internal Spark connectors work, including Hive.

Second is to implement the DataSourceV1 API. This includes implementing interfaces like RelationProvider, FileFormat. This allows users to read or write data using standard DataFrame APIs:
val people: DataFrame = spark.read
  		.format("csv")
  		.load("people.csv")

Third is to implement the DataSourceV2 API. This includes implementing a custom Table interface that represents a dataset, with Traits that allow you to specify implementation of particular operations and optimizations (like predicate pushdown). This also allows users to read or write data using standard DataFrame APIs - Spark detects whether the connector uses V1 or V2 interface and uses correct code paths.

The point of using DataSource APIs for connectors is that they reuse several structures of Spark, including standard user APIs, and LogicalPlans generated for those connectors are implemented: the planner will check whether relevant format is available, and for example for reading from V2 interface will generate DataSourceV2Relation leaf node, that uses relevant Table implementation under the hood coming from particular connector jar.  

To achieve full coverage of Spark operations, OL has to cover implementation of connectors whether they use V1 or V2 interface - it needs to understand the interface’s structure, what LogicalPlan nodes they use and implement support for it in a way that allows us to expose correct dataset naming from each connector - with possibly more metadata.

### What does OpenLineage want to do with Spark connectors?

Right now, OL integration implements support for each connector in the OpenLineage repository. This means OL Spark integration doesn’t only have to understand what LogicalPlan Spark will generate for standard Spark constructs, but also the underlying implementations of DataSource interfaces - for example, OL has an IcebergHandler class that handles getting correct dataset names of Iceberg tables, using internal Iceberg connector classes.

This could be improved for a few reasons. 

First, the connector can change in a way that breaks our interface and they don’t know anything about it. The OpenLineage team also most likely won’t know anything about it until it gets a bug report. 

Second, even when OL receives a bug report, it has to handle the error in a backwards-compatible manner. Users can use different connector versions with different Spark versions on different Scala versions… The matrix of possible configurations vastly exceeds separate implementations for different versions, so the only solution that is realistically doable is using reflection to catch the change and try different code paths. This happens for the BigQuery connector. 

To solve this problem, OL wants to migrate responsibility to exposing lineage metadata directly to connectors, and has created interfaces for Spark connectors to implement. Given implementation of those interfaces, OL Spark integration can just use the exposed data without need to understand the implementation. It allows connectors to test whether they expose correct lineage metadata, and migrate the internals without breaking any OL Spark integration code. 

The interfaces provide a way to integrate OL support for a variety of ways in which Spark connectors are implemented. For example, if connector implements RelationProvider, OL interfaces allow you to extend it with class LineageRelationProvider, that tells the OL Spark integration that it can call getLineageDatasetIdentifier on it, without the need to use other, internal methods of the RelationProvider.

It requires the connector to depend on two maven packages: spark-extension-interfaces and spark-extension-entrypoint. The first one contains the necessary classes to implement support for OpenLineage, however, to maintain compatibility with other connectors (that might rely on a different version of the same jar) the relocation of the package is required. The second package, spark-extension-entrypoint acts like a “pointer” for the actual implementation in the connector, allowing OpenLineage-Spark integration use those relocated classes. 

The detailed documentation for interfaces is [here](https://openlineage.io/docs/development/developing/spark/built_in_lineage/). 
