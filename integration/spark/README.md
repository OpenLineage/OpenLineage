# OpenLineage Spark Listener

The OpenLineage Spark Agent uses jvm instrumentation to emit OpenLineage metadata.

## Installation

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-spark</artifactId>
    <version>0.20.4</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-spark:0.20.4'
```

## Getting started

### Quickstart
The fastest way to get started testing Spark and OpenLineage is to use the docker-compose files included
in the project. From the Spark integration directory ($OPENLINEAGE_ROOT/integration/spark), execute
```bash
docker-compose up
```
This will start Marquez as an Openlineage client and Jupyter Spark notebook on localhost:8888. On startup, the notebook container logs will show a list of URLs
including an access token, such as
```bash
notebook_1  |     To access the notebook, open this file in a browser:
notebook_1  |         file:///home/jovyan/.local/share/jupyter/runtime/nbserver-9-open.html
notebook_1  |     Or copy and paste one of these URLs:
notebook_1  |         http://abc12345d6e:8888/?token=XXXXXX
notebook_1  |      or http://127.0.0.1:8888/?token=XXXXXX
```
Copy the URL with the localhost IP and paste it into your browser window to begin creating a new Jupyter
Spark notebook (see the [https://jupyter-docker-stacks.readthedocs.io/en/latest/](docs) for info on
using the Jupyter docker image).

# OpenLineageSparkListener as a plain Spark Listener
The SparkListener can be referenced as a plain Spark Listener implementation.

Create a new notebook and paste the following into the first cell:
```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master('local')
         .appName('sample_spark')
         .config('spark.jars.packages', 'io.openlineage:openlineage-spark:0.20.4')
         .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
         .config('spark.openlineage.url', 'http://{openlineage.client.host}/api/v1/namespaces/spark_integration/')
         .getOrCreate())
```
To use the local jar, you can build it with
```bash
gradle shadowJar
```
then reference it in the Jupyter notebook with the following (note that the jar should be built
*before* running the `docker-compose up` step or docker will just mount a dummy folder; once the
`build/libs` directory exists, you can repeatedly build the jar without restarting the jupyter
container):
```python
from pyspark.sql import SparkSession

file = "/home/jovyan/openlineage/libs/openlineage-spark-0.20.4.jar"

spark = (SparkSession.builder.master('local').appName('rdd_to_dataframe')
             .config('spark.jars', file)
             .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.+')
             .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
             .config('spark.openlineage.transport.type', 'http')
             .config('spark.openlineage.transport.url', 'http://{openlineage.client.host}/api/v1/namespaces/spark_integration/')
             .getOrCreate())
```

## Arguments

### Spark Listener
The SparkListener reads its configuration from SparkConf parameters. These can be specified on the
command line or in the `conf/spark-defaults.conf` file.

The following parameters can be specified in the Spark configuration:

> **_NOTE:_** The `console` transport mode does not require any additional config so it's preferable for debug or first time set up. Its enabled by setting `spark.openlineage.transport.type` value to `console`.

### General

Parameters configuring the Spark integration

| Parameter                                | Definition                                                                                                                                          | Example                             |
------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------
| spark.openlineage.transport.type         | The transport type used for event emit, default type is `http`                                                                                      | http                                |
| spark.openlineage.namespace              | The default namespace to be applied for any jobs submitted                                                                                          | MyNamespace                         |
| spark.openlineage.parentJobName          | The job name to be used for the parent job facet                                                                                                    | ParentJobName                       |
| spark.openlineage.parentRunId            | The RunId of the parent job that initiated this Spark job                                                                                           | xxxx-xxxx-xxxx-xxxx                 |
| spark.openlineage.appName                | Custom value overwriting Spark app name in events                                                                                                   | AppName                             |
| spark.openlineage.facets.disabled        | List of facets to disable, enclosed in `[]` (required from 0.21.x) and separated by `;`, default is `[spark_unknown;]` (currently must contain `;`) | \[spark_unknown;spark.logicalPlan\] |

### HTTP

| Parameter                                     | Definition                                                                                                                | Example               |
-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|-----------------------
| spark.openlineage.transport.endpoint          | Path to resource                                                                                                          | /api/v1/lineage       |
| *DEPRECATED* spark.openlineage.version        | The API version of the OpenLineage API server (unavailable from x.x.x)                                                    | 1                     |
| spark.openlineage.transport.apiKey            | An API key to be used when sending events to the OpenLineage server                                                       | abcdefghijk           |
| *DEPRECATED* spark.openlineage.apiKey         | An API key to be used when sending events to the OpenLineage server (unavailable from x.x.x)                              | abcdefghijk           |
| spark.openlineage.transport.timeout           | Timeout for sending OpenLineage info in milliseconds                                                                      | 5000                  |
| *DEPRECATED* spark.openlineage.timeout        | Timeout for sending OpenLineage info in milliseconds (unavailable from x.x.x)                                             | 5000                  |
| spark.openlineage.transport.urlParams.xyz     | A URL parameter (replace xyz) and value to be included in requests to the OpenLineage API server                          | abcdefghijk           |
| *DEPRECATED* spark.openlineage.url.param.xyz  | A URL parameter (replace xyz) and value to be included in requests to the OpenLineage API server (unavailable from x.x.x) | abcdefghijk           |
| spark.openlineage.transport.url               | The hostname of the OpenLineage API server where events should be reported, it can have other properties embeded          | http://localhost:5000 |
| *DEPRECATED* spark.openlineage.transport.host | The hostname of the OpenLineage API server where events should be reported (unavailable from x.x.x)                       | http://localhost:5000 |

##### URL

You can supply http parameters using values in url, the parsed `spark.openlineage.*` properties are located in url as follows:

`{transport.url}/{transport.endpoint}/namespaces/{namespace}/jobs/{parentJobName}/runs/{parentRunId}?app_name={appName}&api_key={transport.apiKey}&timeout={transport.timeout}&xxx={transport.urlParams.xxx}`

example:

`http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx?app_name=app&api_key=abc&timeout=5000&xxx=xxx`

### Kinesis 
If `spark.openlineage.transport.type` is set to `kinesis`, then the below parameters would be read and used when building KinesisProducer.
Also, KinesisTransport depends on you to provide artifact `com.amazonaws:amazon-kinesis-producer:0.14.0` or compatible on your classpath.

| Parameter                                     | Definition                                                                                                                                                                                   | Example          |
-----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------
| spark.openlineage.transport.streamName        | Required, the streamName of the Kinesis Stream                                                                                                                                               | some-stream-name |
| spark.openlineage.transport.region            | Required, the region of the stream                                                                                                                                                           | us-east-2        |
| spark.openlineage.transport.roleArn           | Optional, the roleArn which is allowed to read/write to Kinesis stream                                                                                                                       | some-role-arn    |
| spark.openlineage.transport.properties.[xxx]  | Optional, the [xxx] is property of [Kinesis allowd properties](https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties) | 1                |

### Kafka 
If `spark.openlineage.transport.type` is set to `kafka`, then the below parameters would be read and used when building KafkaProducer.

| Parameter                                    | Definition                                      | Example    |
----------------------------------------------|-------------------------------------------------|------------
| spark.openlineage.transport.topicName        | Required, name of the topic                     | topic-name |
| spark.openlineage.transport.localServerId    | Required, id of local server                    | xxxxxxxx   |
| spark.openlineage.transport.properties.[xxx] | Optional, the [xxx] is property of Kafka client | 1          |


# Build

## Java 8

Testing requires a Java 8 JVM to test the Scala Spark components.

`export JAVA_HOME=`/usr/libexec/java_home -v 1.8`

## Preparation

Before testing or building the jar, run the following command from the client/java directory
to install the openlineage-java jar, on which this module depends:

```sh
$ ./gradlew publishToMavenLocal
```

## Testing

To run the tests, from the current directory run:

```sh
./gradlew test
```

To run the integration tests, from the current directory run:

```sh
./gradlew integrationTest
```

## Build jar

```sh
./gradlew shadowJar
```

# Extending
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

###[`OpenLineageEventHandlerFactory`](shared/src/main/java/io/openlineage/spark/api/OpenLineageEventHandlerFactory.java)
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

See the [`OpenLineageEventHandlerFactory` javadocs](shared/src/main/java/io/openlineage/spark/api/OpenLineageEventHandlerFactory.java)
for specifics on each method.


### [`QueryPlanVisitor`](shared/src/main/java/io/openlineage/spark/api/QueryPlanVisitor.java)
QueryPlanVisitors evaluate nodes of a Spark `LogicalPlan` and attempt to generate `InputDataset`s or
`OutputDataset`s from the information found in the `LogicalPlan` nodes. This is the most common
abstraction present in the OpenLineage Spark library, and many examples can be found in the
`io.openlineage.spark.agent.lifecycle.plan` package - examples include the
[`BigQueryNodeVisitor`](shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/BigQueryNodeVisitor.java),
the [`KafkaRelationVisitor`](shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/KafkaRelationVisitor.java)
and the [`InsertIntoHiveTableVisitor`](shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/InsertIntoHiveTableVisitor.java).

`QueryPlanVisitor`s implement Scala's `PartialFunction` interface and are tested against every node
of a Spark query's optimized `LogicalPlan`. Each invocation will expect either an `InputDataset`
or an `OutputDataset`. If a node can be either an `InputDataset` or an `OutputDataset`, the
constructor should accept a `DatasetFactory` so that the correct dataset type is generated at
runtime.

`QueryPlanVisitor`s can attach facets to the Datasets created, e.g., `SchemaDatasetFacet` and
`DatasourceDatasetFacet` are typically attached to the dataset when it is created. Custom facets
can also be attached, though `CustomFacetBuilder`s _may_ override facets attached directly to the
dataset.

### [`InputDatasetBuilder`s](shared/src/main/java/io/openlineage/spark/api/AbstractInputDatasetBuilder.java) and [`OutputDatasetBuilder`s](integration/spark/src/main/common/java/io/openlineage/spark/api/AbstractOutputDatasetBuilder.java)
Similar to the `QueryPlanVisitor`s, `InputDatasetBuilder`s and `OutputDatasetBuilder`s are
`PartialFunction`s defined for a specific input (see below for the list of Spark listener events and
scheduler objects that can be passed to a builder) that can generate either an `InputDataset` or an
`OutputDataset`. Though not strictly necessary, the abstract base classes
[`AbstractInputDatasetBuilder`s](shared/src/main/java/io/openlineage/spark/api/AbstractInputDatasetBuilder.java)
and [`AbstractOutputDatasetBuilder`s](shared/src/main/java/io/openlineage/spark/api/AbstractOutputDatasetBuilder.java)
are available for builders to extend.

### [`CustomFacetBuilder`](shared/src/main/java/io/openlineage/spark/api/CustomFacetBuilder.java)
`CustomFacetBuilders` evaluate Spark event types and scheduler objects (see below) to construct custom
facets. `CustomFacetBuilders` are used to create `InputDatsetFacet`s, `OutputDatsetFacet`s,
`DatsetFacet`s, `RunFacet`s, and `JobFacet`s. A few examples can be found in the
[`io.openlineage.spark.agent.facets.builder`](shared/src/main/java/io/openlineage/spark/agent/facets/builder)
package, including the [`ErrorFacetBuilder`](shared/src/main/java/io/openlineage/spark/agent/facets/builder/ErrorFacetBuilder.java)
and the [`LogicalPlanRunFacetBuilder`](shared/src/main/java/io/openlineage/spark/agent/facets/builder/LogicalPlanRunFacetBuilder.java).
`CustomFacetBuilder`s are not `PartialFunction` implementations, but do define the `isDefinedAt(Object)`
method to determine whether a given input is valid for the function. They implement the `BiConsumer`
interface, accepting the valid input argument, and a `BiConsumer<String, Facet>` consumer, which
accepts the name and value of any custom facet that should be attached to the OpenLineage run.
There is no limit to the number of facets that can be reported by a given `CustomFacetBuilder`.
Facet names that conflict will overwrite previously reported facets if they are reported for the
same Spark event.
Though not strictly necessary, the following abstract base classes are available for extension:
* [`AbstractJobFacetBuilder`s](shared/src/main/java/io/openlineage/spark/api/AbstractJobFacetBuilder.java)
* [`AbstractRunFacetBuilder`s](shared/src/main/java/io/openlineage/spark/api/AbstractRunFacetBuilder.java)
* [`AbstractInputDatasetFacetBuilder`s](shared/src/main/java/io/openlineage/spark/api/AbstractInputDatasetFacetBuilder.java)
* [`AbstractOutputDatasetFacetBuilder`s](shared/src/main/java/io/openlineage/spark/api/AbstractOutputDatasetFacetBuilder.java)
* [`AbstractDatasetFacetBuilder`s](shared/src/main/java/io/openlineage/spark/api/AbstractDatasetFacetBuilder.java)

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

## Contributing

If contributing changes, additions or fixes to the Spark integration, please include the following header in any new `.java` files:

```
/* 
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0 
*/
```

A Github Action checks for headers in new `.java` files when pull requests are opened.

Thank you for your contributions to the project!

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project