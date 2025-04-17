---
sidebar_position: 4
title: Flink 1.x
---

## Getting lineage from Flink

:::warning
This is Flink 1.x integration docs. For Flink 2.x integration, please refer to [Flink 2.x integration](flink2.md).
:::

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

## Supported Sources and Sinks

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
    <tr>
      <td>JdbcSource</td>
      <td>JdbcSink</td>
    </tr>
    <tr>
      <td>CassandraSource</td>
      <td>CassandraSink</td>
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

OpenLineage jar needs to be present on `JobManager`. It also requires running in `application mode` with setting `execution.attached: true`.
If `execution.attached` is false, we don't receive proper information about job completion.

When the `JobListener` is configured, you need to point the OpenLineage integration where the events should end up. 
If you're using `Marquez`, simplest way to do that is to set up `OPENLINEAGE_URL` environment
variable to `Marquez` URL. More advanced settings are [in the client documentation.](../../client/java/java.md).
