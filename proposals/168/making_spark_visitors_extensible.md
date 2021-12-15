---
Author: Michael Collado
Created: December 14
Issue: https://github.com/OpenLineage/OpenLineage/issues/168
---

# Purpose
Users need to write custom extensions to the Spark OpenLineage library in order to support custom datasources, add
custom job facets, and to augment events with application-specific logic. Currently, all of the logic for reporting
OpenLineage events is contained in the `io.openlineage.spark.agent.lifecycle` package of the java library. This proposal
allows for scanning the classpath to find custom implementations of event handlers that can publish OpenLineage 
components based on application events.

# Implementation

Most of the code that publishes OpenLineage information from Spark applications uses Spark SQL's `LogicalPlan` to 
locate and extract dataset and job information, such as the location of a dataset in GCS or the name and schema of a
table in Hive or a relational database. Implementations subclass an abstract class called `QueryPlanVisitor`, which 
is a `PartialFunction` that can convert specific implementations of `LogicalPlan` into `OpenLineage.Dataset`s. 
However, some information necessary to construct a complete OpenLineage event is not found in the `LogicalPlan`. Simple
examples include the number of records and bytes read and/or written during execution. That information is found in the
`TaskMetrics` object bound to a `StageInfo` reported at the time of a `SparkListenerStageCompleted` event. Some 
information, such as the `HadoopRDD` being processed is found in the `RDD` bound to a `Stage` (not a `StageInfo`).
Thus, adding support for customer `QueryPlanVisitor`s will not allow full customization for users who are interested in
publishing custom dataset or job/run facets. 

To allow full customization, we can introduce the following interfaces
```java
/**
 * Interface for custom facet builders. `isDefinedAt` determines the type of event supported by the 
 */
public interface CustomFacetBuilder<T, F> {

    /**
     * @param event
     * @return true if this function is defined for the argument
     */
    boolean isDefinedAt(T event);

    /**
     * Handle the given event, writing any resulting facets to the {@link java.util.function.BiConsumer}, which accepts
     * the name of the facet and the facet itself.
     */
    void accept(T event, BiConsumer<String, ? super F> consumer);

}

/**
 * Factory for the builders that generate OpenLineage components and facets from Spark events.
 */
public interface OpenLineageEventListenerFactory {

    /**
     * Set the {@link OpenLineageContext} to be used during construction of the below builders
     * @param context
     */
    default void setOpenLineageContext(OpenLineageContext context) {} // does nothing by default

    /**
     * @return a collection of {@link io.openlineage.spark.api.QueryPlanVisitor}s that can construct `InputDataset`s.
     */
    default Collection<QueryPlanVisitor<LogicalPlan>> createInputDatasetQueryPlanVisitors() {
        return Collections.emptyList();
    }

    default Collection<QueryPlanVisitor<LogicalPlan>> createOutputDatasetQueryPlanVisitors() {
        return Collections.emptyList();
    }

    default Collection<PartialFunction<Object, List<OpenLineage.InputDatasetBuilder>>> createInputDatasetBuilder() {
        return PartialFunction.empty();
    }

    default Collection<PartialFunction<Object, List<OpenLineage.InputDatasetBuilder>>> createOutputDatasetBuilder() {
        return Collections.emptyList();
    }

    default Collection<CustomFacetBuilder<Object, OpenLineage.InputDatasetFacet>> createInputDatasetFacetBuilders() {
        return Collections.emptyList();
    }

    default Collection<CustomFacetBuilder<Object, OpenLineage.OutputDatasetFacet>> createOutputDatasetFacetBuilders() {
        return Collections.emptyList();
    }

    default Collection<CustomFacetBuilder<Object, OpenLineage.DatasetFacet>> createDatasetFacetBuilders() {
        return Collections.emptyList();
    }

    default Collection<CustomFacetBuilder<Object, OpenLineage.RunFacet>> createRunFacetBuilders() {
        return Collections.emptyList();
    }

    default Collection<CustomFacetBuilder<Object, OpenLineage.JobFacet>> createJobFacetBuilders() {
        return Collections.emptyList();
    }

}
```

The `OpenLineageEventListenerFactory` interface will be implemented by users and will return `Collection`s of facet 
builders and dataset builders. `Collection` implies that any ordering of the returned collections is not guaranteed nor
should any predictable iteration order be expected. 

Users will supply their `OpenLineageEventListenerFactory` implementation by following the conventions outlined in the
`java.util.ServiceLoader` documentation - a file named `META-INF/services/io.openlineage.spark.api.OpenLineageEventListenerFactory`
must be included in a jar on the classpath. Each line in the file will contain the fully qualified name of an 
implementing class.

Implementing services must have a zero-arg constructor, which is invoked by the `java.util.ServiceLoader.Provider`.
Some builders will need access to the `SparkContext` or to be able to delegate to other builders (e.g., the
`SaveIntoDataSourceCommandVisitor` unwraps the `LogicalRelation` from the `dataSource` field, then delegates to an
implementation that knows how to handle the specific `LogicalRelation` returned). For this purpose, we'll also introduce
an `OpenLineageContext` class, defined as

```java
@Value
public class OpenLineageContext {
    Collection<QueryPlanVisitor<LogicalPlan>> inputDatasetQueryPlanVisitors;
    Collection<QueryPlanVisitor<LogicalPlan>> outputDatasetQueryPlanVisitors;
    Collection<CustomFacetBuilder<Object, OpenLineage.InputDatasetFacet>> inputDatasetFacetBuilders;
    Collection<CustomFacetBuilder<Object, OpenLineage.OutputDatasetFacet>> outputDatasetFacetBuilders;
    Collection<CustomFacetBuilder<Object, OpenLineage.DatasetFacet>> datasetFacetBuilders;
    Collection<CustomFacetBuilder<Object, OpenLineage.RunFacet>> runFacetBuilders;
    Collection<CustomFacetBuilder<Object, OpenLineage.JobFacet>> jobFacetBuilders;
    SparkContext sparkContext;
}
```

The `OpenLineageEventListenerFactory#setOpenLineageContext` method will be called once the concrete service 
implementation has been constructed, before the first `createX` method is called.

Each of the `createX` methods will be called exactly once during a Spark application. There's no guarantee regarding the
order in which the `create` methods will be called. Thus, the constructed builders should make no assumptions about the
state of the `OpenLineageContext` when the `create` method is called. E.g., the `inputDatasetFacetBuilders` should not 
assume that all of the `inputDatasetQueryPlanVisitors` have been instantiated or that the `Collection` returned by the
getter has been fully populated. Builders that need to depend on the ability to delegate to other builders should store
a reference to the `OpenLineageContext` and call the appropriate getter at the time the delegate is actually needed.

The builders returned are all some form of `PartialFunction` - they must all define an `isDefinedAt(T event)` method
which determines the type of argument the builder can respond to. The `createInputDatasetBuilder` and 
`createOutputDatasetBuilder` methods return `PartialFunction`s that construct either an `InputDatasetBuilder` or an
`OutputDatasetBuilder`, respectively. Builders are returned so that facets constructed by the other `PartialFunction`s
can be attached to the dataset before the immutable `InputDataset` or `OutputDataset` is returned. It is expected that
the `CustomFacet`s returned are already immutable.

For each `SparkListener` event, an `OpenLineage.RunEvent` will be constructed by 
1. Construct `RunFacet`s by invoking each of the `runFacetBuilders`
2. Construct `JobFacet`s by invoking each of the `jobFacetBuilders`
3. Construct `InputDatasetBuilder`s by invoking each of the `inputDatasetBuilder`s.
4. _If_ any `InputDatasetBuilder`s are returned, invoke the `inputDatasetFacetBuilder`s and attach any returned facets to
   the `InputDatasetBuilder`s
5. _If_ any `InputDatasetBuilder`s are returned, invoke the `datasetFacetBuilder`s and attach any returned facets to 
    the `InputDatasetBuilder`s
6. Construct `OutputDatasetBuilder`s by invoking each of the `outputDatasetBuilder`s.
7. _If_ any `OuptutDatasetBuilder`s are returned, invoke the `outputDatasetFacetBuilder`s and attach any returned facets to
   the `OutputDatasetBuilder`s
5. _If_ any `OutputDatasetBuilder`s are returned, invoke the `datasetFacetBuilder`s and attach any returned facets to
   the `OutputDatasetBuilder`s

`InputDatasetFacet`s and `OutputDatasetFacet`s will be attached to _any_ `InputDatasetBuilder` or `OutputDatasetBuilder`
found for the event. This is because facets may be constructed from generic information that is not specifi**cally tied
to a Dataset. For example, `OutputStatisticsOutputDatasetFacet`s are created from `TaskMetrics` attached to** the last
`StageInfo` for a given job execution. However, the `OutputDataset` is constructed by reading the `LogicalPlan`. There's
no way to tie the output metrics in the `StageInfo` to the `OutputDataset` in the `LogicalPlan` except by inference.
Similarly, input metrics can be found in the `StageInfo` for the stage that reads a dataset and the `InputDataset` can
usually be constructed by walking the `RDD` dependency tree for that `Stage` and finding a `FileScanRDD` or other 
concrete implementation. But while there is typically only one `InputDataset` read in a given stage, there's no 
guarantee of that and the `TaskMetrics` in the `StageInfo` won't disambiguate. If a facet needs to be attached to a
specific dataset, the user must take care to construct both the Dataset and the Facet in the same builder.

For convenience, the following abstract base classes are defined:

```java
// accepts an argument of type T and returns an InputDatasetBuilder
public abstract class AbstractInputDatasetBuilder<T>{}
```

```java
// accepts an argument of type T and returns an InputDatasetFacet
public abstract class AbstractInputDatasetFacetBuilder<T>{}
```

```java
// accepts an argument of type T and returns an OutputDatasetBuilder
public abstract class AbstractOutputDatasetBuilder<T>{}
```

```java
// accepts an argument of type T and returns an OutputDatasetFacet
public abstract class AbstractOutputDatasetFacetBuilder<T>{}
```

```java
// accepts an argument of type T and returns a RunFacet
public abstract class AbstractRunFacetBuilder<T>{}
```

```java
// accepts an argument of type T and returns a JobFacet
public abstract class AbstractJobFacetBuilder<T>{}
```

By default, the `isDefinedAt` method is implemented to support a concrete type argument for the T parameter. E.g., a
concrete defined as `MyRDDFacet extends AbstractJobFacetBuilder<RDD>` will be _defined_ for any instance of `RDD`.

Currently, the types that will be passed to builders include
* org.apache.spark.scheduler.StageInfo
* org.apache.spark.scheduler.Stage
* org.apache.spark.scheduler.ActiveJob
* org.apache.spark.sql.execution.QueryExecution
* org.apache.spark.rdd.RDD

`RDD` chains will be _flattened_ so each `RDD` dependency is passed to the builders one at a time. This means a builder
can directly specify the type of `RDD` it handles, such as a `HadoopRDD` or a `FileScanRDD`, without having to check
the dependencies of every `MapPartitionRDD` or `SQLExecutionRDD`.

Some simple examples of builder implementations are
```java
public class FileScanInputDatasetBuilder extends AbstractInputDatasetBuilder<FileScanRDD> {
    private final SparkContext sparkContext;

    public FileScanInputDatasetBuilder(OpenLineage openLineage, SparkContext sparkContext) {
        super(openLineage);
        this.sparkContext = sparkContext;
    }

    @Override
    public List<OpenLineage.InputDatasetBuilder> apply(FileScanRDD rdd) {
        return ScalaConversionUtils.fromSeq(rdd.filePartitions())
                .stream()
                .flatMap(fp -> Arrays.stream(fp.files()))
                .map(file -> {
                    Path p = new Path(file.filePath());
                    try {
                        if (p.getFileSystem(sparkContext.hadoopConfiguration()).isFile(p)) {
                            return p.getParent();
                        } else {
                            return p;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).map(Path::toUri)
                .distinct()
                .map(uri -> PathUtils.fromURI(uri, "file"))
                .map(dsi -> openLineage.newInputDatasetBuilder().namespace(dsi.getNamespace()).name(dsi.getName()))
                .collect(Collectors.toList());
    }
}
```

```java
public class OutputStatisticsFacetBuilder implements CustomFacetBuilder<StageInfo, OpenLineage.OutputStatisticsOutputDatasetFacet> {
    private final OpenLineage openLineage;

    public OutputStatisticsFacetBuilder(OpenLineage openLineage) {
        this.openLineage = openLineage;
    }

    @Override
    public void accept(StageInfo event, BiConsumer<String, ? super OpenLineage.OutputStatisticsOutputDatasetFacet> consumer) {
        OpenLineage.OutputStatisticsOutputDatasetFacet statsFacet =
                openLineage.newOutputStatisticsOutputDatasetFacet(
                        event.taskMetrics().outputMetrics().recordsWritten(),
                        event.taskMetrics().outputMetrics().bytesWritten()
                );
        consumer.accept("outputStatistics", statsFacet);
    }
}
```