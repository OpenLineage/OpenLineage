---
sidebar_position: 9
title: Extending
---

The Spark library is intended to support extension via custom implementations of a handful
of interfaces. Nearly every extension interface extends or mimics Scala's `PartialFunction`. The
`isDefinedAt(Object x)` method determines whether a given input is a valid input to the function.
A default implementation of `isDefinedAt(Object x)` is provided, which checks the generic type
arguments of the concrete class, if concrete type arguments are given, and determines if the input
argument matches the generic type. For example, the following class is automatically defined for an
input argument of type `MyDataset`.

```
class MyDatasetDetector extends QueryPlanVisitor<MyDataset, OutputDataset> {
}
```

## API
The following APIs are still evolving and may change over time based on user feedback.

### [`OpenLineageEventHandlerFactory`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/OpenLineageEventHandlerFactory.java)
This interface defines the main entrypoint to the extension codebase. Custom implementations
are registered by following Java's [`ServiceLoader` conventions](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html).
A file called `io.openlineage.spark.api.OpenLineageEventHandlerFactory` must exist in the
application or jar's `META-INF/service` directory. Each line of that file must be the fully
qualified class name of a concrete implementation of `OpenLineageEventHandlerFactory`. More than one
implementation can be present in a single file. This might be useful to separate extensions that
are targeted toward different environments - e.g., one factory may contain Azure-specific extensions,
while another factory may contain GCP extensions.

The `OpenLineageEventHandlerFactory` interface makes heavy use of default methods. Implementations
may override any or all of the following methods
```java
/**
 * Return a collection of QueryPlanVisitors that can generate InputDatasets from a LogicalPlan node
 */
Collection<PartialFunction<LogicalPlan, List<InputDataset>>> createInputDatasetQueryPlanVisitors(OpenLineageContext context);

/**
 * Return a collection of QueryPlanVisitors that can generate OutputDatasets from a LogicalPlan node
 */
Collection<PartialFunction<LogicalPlan, List<OutputDataset>>> createOutputDatasetQueryPlanVisitors(OpenLineageContext context);

/**
 * Return a collection of PartialFunctions that can generate InputDatasets from one of the
 * pre-defined Spark types accessible from SparkListenerEvents (see below)
 */
Collection<PartialFunction<Object, List<InputDataset>>> createInputDatasetBuilder(OpenLineageContext context);

/**
 * Return a collection of PartialFunctions that can generate OutputDatasets from one of the
 * pre-defined Spark types accessible from SparkListenerEvents (see below)
 */
Collection<PartialFunction<Object, List<OutputDataset>>> createOutputDatasetBuilder(OpenLineageContext context);

/**
 * Return a collection of CustomFacetBuilders that can generate InputDatasetFacets from one of the
 * pre-defined Spark types accessible from SparkListenerEvents (see below)
 */
Collection<CustomFacetBuilder<?, ? extends InputDatasetFacet>> createInputDatasetFacetBuilders(OpenLineageContext context);

/**
 * Return a collection of CustomFacetBuilders that can generate OutputDatasetFacets from one of the
 * pre-defined Spark types accessible from SparkListenerEvents (see below)
 */
Collection<CustomFacetBuilder<?, ? extends OutputDatasetFacet>>createOutputDatasetFacetBuilders(OpenLineageContext context);

/**
 * Return a collection of CustomFacetBuilders that can generate DatasetFacets from one of the
 * pre-defined Spark types accessible from SparkListenerEvents (see below)
 */
Collection<CustomFacetBuilder<?, ? extends DatasetFacet>> createDatasetFacetBuilders(OpenLineageContext context);

/**
 * Return a collection of CustomFacetBuilders that can generate RunFacets from one of the
 * pre-defined Spark types accessible from SparkListenerEvents (see below)
 */
Collection<CustomFacetBuilder<?, ? extends RunFacet>> createRunFacetBuilders(OpenLineageContext context);

/**
 * Return a collection of CustomFacetBuilders that can generate JobFacets from one of the
 * pre-defined Spark types accessible from SparkListenerEvents (see below)
 */
Collection<CustomFacetBuilder<?, ? extends JobFacet>> createJobFacetBuilders(OpenLineageContext context);
```

See the [`OpenLineageEventHandlerFactory` javadocs](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/OpenLineageEventHandlerFactory.java)
for specifics on each method.


### [`QueryPlanVisitor`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/QueryPlanVisitor.java)
QueryPlanVisitors evaluate nodes of a Spark `LogicalPlan` and attempt to generate `InputDataset`s or
`OutputDataset`s from the information found in the `LogicalPlan` nodes. This is the most common
abstraction present in the OpenLineage Spark library, and many examples can be found in the
`io.openlineage.spark.agent.lifecycle.plan` package - examples include the
[`BigQueryNodeVisitor`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/BigQueryNodeVisitor.java),
the [`KafkaRelationVisitor`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/KafkaRelationVisitor.java)
and the [`InsertIntoHiveTableVisitor`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/InsertIntoHiveTableVisitor.java).

`QueryPlanVisitor`s implement Scala's `PartialFunction` interface and are tested against every node
of a Spark query's optimized `LogicalPlan`. Each invocation will expect either an `InputDataset`
or an `OutputDataset`. If a node can be either an `InputDataset` or an `OutputDataset`, the
constructor should accept a `DatasetFactory` so that the correct dataset type is generated at
runtime.

`QueryPlanVisitor`s can attach facets to the Datasets created, e.g., `SchemaDatasetFacet` and
`DatasourceDatasetFacet` are typically attached to the dataset when it is created. Custom facets
can also be attached, though `CustomFacetBuilder`s _may_ override facets attached directly to the
dataset.

Spark job's naming logic appends output dataset's identifier as job suffix. In order to provide a job suffix, a `QueryPlanVisitor` 
needs to implement [`JobNameSuffixProvider`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/JobNameSuffixProvider.java)
interface. Otherwise no suffix will be appended. Job suffix should contain human-readable name
of the dataset so that consumers of OpenLineage events can correlate events with particular
Spark actions within their code. The logic to extract dataset name should not depend on the existence
of the dataset as in case of creating new dataset it may not exist at the moment of assigning job suffix.
In most cases, the suffix should contain spark catalog, database and table separated by `.` which shall be
extracted from LogicalPlan nodes properties.

### [`InputDatasetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractInputDatasetBuilder.java) and [`OutputDatasetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/common/java/io/openlineage/spark/api/AbstractOutputDatasetBuilder.java)
Similar to the `QueryPlanVisitor`s, `InputDatasetBuilder`s and `OutputDatasetBuilder`s are
`PartialFunction`s defined for a specific input (see below for the list of Spark listener events and
scheduler objects that can be passed to a builder) that can generate either an `InputDataset` or an
`OutputDataset`. Though not strictly necessary, the abstract base classes
[`AbstractInputDatasetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractInputDatasetBuilder.java)
and [`AbstractOutputDatasetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractOutputDatasetBuilder.java)
are available for builders to extend.

Spark job's naming logic appends output dataset's identifier as job suffix. 
In order to provide a job suffix, a `OutputDatasetBuilder` needs to implement [`JobNameSuffixProvider`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/JobNameSuffixProvider.java)
interface. Otherwise no suffix will be appended. Job suffix should contain human-readable name 
of the dataset so that consumers of OpenLineage events can correlate events with particular
Spark actions within their code. The logic to extract dataset name should not depend on the existence
of the dataset as in case of creating new dataset it may not exist at the moment of assigning job suffix.
In most cases, the suffix should contain spark catalog, database and table separated by `.` which shall be
extracted from LogicalPlan nodes properties.

### [`CustomFacetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/CustomFacetBuilder.java)
`CustomFacetBuilders` evaluate Spark event types and scheduler objects (see below) to construct custom
facets. `CustomFacetBuilders` are used to create `InputDatsetFacet`s, `OutputDatsetFacet`s,
`DatsetFacet`s, `RunFacet`s, and `JobFacet`s. A few examples can be found in the
[`io.openlineage.spark.agent.facets.builder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/builder)
package, including the [`ErrorFacetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/builder/ErrorFacetBuilder.java)
and the [`LogicalPlanRunFacetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/builder/LogicalPlanRunFacetBuilder.java).
`CustomFacetBuilder`s are not `PartialFunction` implementations, but do define the `isDefinedAt(Object)`
method to determine whether a given input is valid for the function. They implement the `BiConsumer`
interface, accepting the valid input argument, and a `BiConsumer<String, Facet>` consumer, which
accepts the name and value of any custom facet that should be attached to the OpenLineage run.
There is no limit to the number of facets that can be reported by a given `CustomFacetBuilder`.
Facet names that conflict will overwrite previously reported facets if they are reported for the
same Spark event.
Though not strictly necessary, the following abstract base classes are available for extension:
* [`AbstractJobFacetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractJobFacetBuilder.java)
* [`AbstractRunFacetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractRunFacetBuilder.java)
* [`AbstractInputDatasetFacetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractInputDatasetFacetBuilder.java)
* [`AbstractOutputDatasetFacetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractOutputDatasetFacetBuilder.java)
* [`AbstractDatasetFacetBuilder`](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractDatasetFacetBuilder.java)

Input/Output/Dataset facets returned are attached to _any_ Input/Output Dataset found for a given
Spark event. Typically, a Spark job only has one `OutputDataset`, so any `OutputDatasetFacet`
generated will be attached to that `OutputDataset`. However, Spark jobs often have multiple
`InputDataset`s. Typically, an `InputDataset` is read within a single Spark `Stage`, and any metrics
pertaining to that dataset may be present in the `StageInfo#taskMetrics()` for that `Stage`.
Accumulators pertaining to a dataset should be reported in the task metrics for a stage so that the
`CustomFacetBuilder` can match against the `StageInfo` and retrieve the task metrics for that stage
when generating the `InputDatasetFacet`. Other facet information is often found by analyzing the
`RDD` that reads the raw data for a dataset. `CustomFacetBuilder`s that generate these facets should
be defined for the specific subclass of `RDD` that is used to read the target dataset - e.g.,
`HadoopRDD`, `BigQueryRDD`, or `JdbcRDD`.

### Function Argument Types
`CustomFacetBuilder`s and dataset builders can be defined for the following set of Spark listener
event types and scheduler types:

* `org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart`
* `org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd`
* `org.apache.spark.scheduler.SparkListenerJobStart`
* `org.apache.spark.scheduler.SparkListenerJobEnd`
* `org.apache.spark.rdd.RDD`
* `org.apache.spark.scheduler.Stage`
* `org.apache.spark.scheduler.StageInfo`
* `org.apache.spark.scheduler.ActiveJob`

Note that `RDD`s are "unwrapped" prior to being evaluated by builders, so there's no need to, e.g.,
check a `MapPartitionsRDD`'s dependencies. The `RDD` for each `Stage` can be evaluated when a
`org.apache.spark.scheduler.SparkListenerStageCompleted` event occurs. When a
`org.apache.spark.scheduler.SparkListenerJobEnd` event is encountered, the last `Stage` for the
`ActiveJob` can be evaluated.

## Spark extensions' built-in lineage extraction

Spark ecosystem comes with a plenty of pluggable extensions like iceberg, delta or spark-bigquery-connector
to name a few. Extensions modify logical plan of the job and inject its own classes from which lineage shall be
extracted. This is adding extra complexity, as it makes `openlineage-spark` codebase
dependent on the extension packages. The complexity grows more when multiple versions
of the same extension need to be supported.

### Spark DataSource V2 API Extensions

Some extensions rely on Spark DataSource V2 API and implement TableProvider, Table, ScanBuilder etc.
that are used within Spark to create `DataSourceV2Relation` instances.

A logical plan node `DataSourceV2Relation` contains `Table` field with a properties map of type
`Map<String, String>`. `openlineage-spark` uses this map to extract dataset information for lineage
event from `DataSourceV2Relation`. It is checking for the properties `openlineage.dataset.name` and
`openlineage.dataset.namespace`. If they are present, it uses them to identify a dataset. Please
be aware that namespace and name need to conform to [naming convention](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md).

Properties can be also used to pass any dataset facet. For example:
```
openlineage.dataset.facets.customFacet={"property1": "value1", "property2": "value2"}
```
will enrich dataset with `customFacet`:
```json
"inputs": [{
    "name": "...",
    "namespace": "...",
    "facets": {
        "customFacet": {
            "property1": "value1",
            "property2": "value2",
            "_producer": "..."
        },
        "schema": { }
}]
```

The approach can be used for standard facets
from OpenLineage spec as well. `schema` does not need to be passed through the properties as
it is derived within `openlineage-spark` from `DataSourceV2Relation`. Custom facets are automatically
filled with `_producer` field.