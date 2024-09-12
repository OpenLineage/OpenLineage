import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::info
This feature is available in OpenLineage 1.11 and above
:::

To ease the operational experience of using the OpenLineage integrations, this document details the metrics collected by the Java client and the configuration settings for various metric backends.

### Metrics collected by Java Client

The following table outlines the metrics collected by the OpenLineage Java client, which help in monitoring the integration's performance:

| Metric                              | Definition                                            | Type   |
|-------------------------------------|-------------------------------------------------------|--------|
| `openlineage.emit.start`            | Number of events the integration started to send      | Counter|
| `openlineage.emit.complete`         | Number of events the integration completed sending    | Counter|
| `openlineage.emit.time`             | Time spent on emitting events                         | Timer  |
| `openlineage.circuitbreaker.engaged`| Status of the Circuit Breaker (engaged or not)        | Gauge  |

## Metric Backends

OpenLineage uses [Micrometer](https://micrometer.io) for metrics collection, similar to how SLF4J operates for logging. Micrometer provides a facade over different metric backends, allowing metrics to be dispatched to various destinations.

### Configuring Metric Backends

Below are the available backends and potential configurations using Micrometer's facilities.

### StatsD

Full configuration options for StatsD can be found in the [Micrometer's StatsDConfig implementation](https://github.com/micrometer-metrics/micrometer/blob/main/implementations/micrometer-registry-statsd/src/main/java/io/micrometer/statsd/StatsdConfig.java). 

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
metrics:
  type: statsd
  flavor: datadog
  host: localhost
  port: 8125
```
</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                            | Definition                            | Example     |
--------------------------------------|---------------------------------------|-------------
| spark.openlineage.metrics.type | Metrics type selected         | statsd |
| spark.openlineage.metrics.flavor | Flavor of StatsD configuration                      | datadog |
| spark.openlineage.metrics.host | Host that receives StatsD metrics | localhost        |
| spark.openlineage.metrics.port | Port that receives StatsD metrics | 8125        |

</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                            | Definition                            | Example     |
--------------------------------------|---------------------------------------|-------------
| openlineage.metrics.type | Metrics type selected         | statsd |
| openlineage.metrics.flavor | Flavor of StatsD configuration                      | datadog |
| openlineage.metrics.host | Host that receives StatsD metrics | localhost        |
| openlineage.metrics.port | Port that receives StatsD metrics | 8125        |

</TabItem>
</Tabs>
