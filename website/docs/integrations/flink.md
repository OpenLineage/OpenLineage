---
sidebar_position: 4
title: Apache Flink
---


**Apache Flink** is one of the most popular stream processing frameworks. Apache Flink jobs run on clusters, 
which are composed of two types of nodes: `TaskManagers` and `JobManagers`. While clusters typically consists of 
multiple `TaskManagers`, only reason to run multiple JobManagers is high availability. The jobs are _submitted_
to `JobManager` by `JobClient`, that compiles user application into dataflow graph which is understandable by `JobManager`. 
`JobManager` then coordinates job execution: it splits the parallel units of a job
to `TaskManagers`, manages heartbeats, triggers checkpoints, reacts to failures and much more.

Apache Flink has multiple deployment modes - Session Mode, Application Mode and Per-Job mode. The most popular
are Session Mode and Application Mode. Session Mode consists of a `JobManager` managing multiple jobs sharing single
Flink cluster. In this mode, `JobClient` is executed on a machine that submits the job to the cluster.

Application Mode is used where cluster is utilized for a single job. In this mode, `JobClient`, where the main method runs,
is executed on the `JobManager`.

Flink jobs read data from `Sources` and write data to `Sinks`. In contrast to systems like Apache Spark, Flink jobs can write 
data to multiple places - they can have multiple `Sinks`.

## Getting lineage from Flink

OpenLineage utilizes Flink's `JobListener` interface. This interface is used by Flink to notify user of job submission,
successful finish of job, or job failure. Implementations of this interface are executed on `JobClient`. 

When OpenLineage listener receives information that job was submitted, it extracts `Transformations` from job's 
`ExecutionEnvironment`. The `Transformations` represent logical operations in the dataflow graph; they are composed
of both Flink's built-in operators, but also user-provided `Sources`, `Sinks` and functions. To get the lineage,
OpenLineage integration processes dataflow graph. Currently, OpenLineage is interested only in information contained 
in `Sources` and `Sinks`, as they are the places where Flink interacts with external systems. 

After job submission, OpenLineage integration starts actively listening to checkpoints - this gives insight into 
whether the job runs properly.

## Limitations

Currently, OpenLineage's Flink integration is limited to getting information from jobs running in Application Mode.

OpenLineage integration extracts lineage only from following `Sources` and `Sinks`:

<table>
  <tbody>
    <tr>
      <th>Sources</th>
      <th>Sinks</th>
    </tr>
    <tr>
      <td>KafkaSource</td>
      <td>KafkaSink (1)</td>
    </tr>
    <tr>
      <td>FlinkKafkaConsumer</td>
      <td>FlinkKafkaProducer</td>
    </tr>
    <tr>
      <td>IcebergFlinkSource</td>
      <td>IcebergFlinkSink</td>
    </tr>
  </tbody>
</table>

We expect this list to grow as we add support for more connectors.

(1) KafkaSink supports sinks that write to a single topic as well as multi topic sinks. The 
limitation for multi topic sink is that: topics need to have the same schema and implementation
of `KafkaRecordSerializationSchema` must extend `KafkaTopicsDescriptor`. 
Methods `isFixedTopics` and `getFixedTopics` from `KafkaTopicsDescriptor` are used to extract multiple topics 
from a sink. 

## Usage

In your job, you need to set up `OpenLineageFlinkJobListener`.

For example:
```java
JobListener listener = OpenLineageFlinkJobListener.builder()
    .executionEnvironment(streamExecutionEnvironment)
    .build();
streamExecutionEnvironment.registerJobListener(listener);
```

Also, OpenLineage needs certain parameters to be set in `flink-conf.yaml`:

<table>
  <tbody>
<tr>
  <th>Configuration Key</th>
  <th>Description</th>
  <th>Expected Value</th>
  <th>Default</th>
</tr>
<tr>
  <td>execution.attached</td>
  <td>This setting needs to be true if OpenLineage is to detect job start and failure</td>
  <td>true</td>
  <td>false</td>
</tr>
  </tbody>
</table>

OpenLineage jar needs to be present on `JobManager`.

When the `JobListener` is configured, you need to point the OpenLineage integration where the events should end up. 
If you're using `Marquez`, simplest way to do that is to set up `OPENLINEAGE_URL` environment
variable to `Marquez` URL. More advanced settings are [in the client documentation.](../client/java/java.md).

## Configuring Openlineage connector

Flink Openlineage connector utilizes standard [Java client for Openlineage](https://github.com/OpenLineage/OpenLineage/tree/main/client/java)
and allows all the configuration features present there to be used. The configuration can be passed with:
 * `openlineage.yml` file with a environment property `OPENLINEAGE_CONFIG` being set and pointing to configuration file. File structure and allowed options are described [here](https://github.com/OpenLineage/OpenLineage/tree/main/client/java#configuration).
 * Standard Flink configuration with the parameters defined below.

### Flink Configuration parameters

The following parameters can be specified:

| Parameter                                | Definition                                                                                                                                                                          | Example                                 |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| openlineage.transport.type               | The transport type used for event emit, default type is `console`                                                                                                                   | http                                    |
| openlineage.facets.disabled              | List of facets to disable, enclosed in `[]` (required from 0.21.x) and separated by `;`, default is `[spark_unknown;spark.logicalPlan;]` (currently must contain `;`)               | \[some_facet1;some_facet1\]             |
| openlineage.job.owners.<ownership-type\> | Specifies ownership of the job. Multiple entries with different types are allowed. Config key name and value are used to create job ownership type and name (available since 1.13). | openlineage.job.owners.team="Some Team" |

## Transports

import Transports from '@site/docs/client/java/partials/java_transport.md';

<Transports/>

## Circuit Breakers

import CircuitBreakers from '@site/docs/client/java/partials/java_circuit_breaker.md';

<CircuitBreakers/>

