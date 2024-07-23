import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::info
This feature is available in OpenLineage versions >= 1.9.0.
:::

To prevent from over-instrumentation OpenLineage integration provides a circuit breaker mechanism
that stops OpenLineage from creating, serializing and sending OpenLineage events.

### Simple Memory Circuit Breaker

Simple circuit breaker which is working based only on free memory within JVM. Configuration should
contain free memory threshold limit (percentage). Default value is `20%`. The circuit breaker
will close within first call if free memory is low. `circuitCheckIntervalInMillis` parameter is used
to configure a frequency circuit breaker is called. Default value is `1000ms`, when no entry in config.
`timeoutInSeconds` is optional. If set, OpenLineage code execution is terminated when a timeout
is reached (added in version 1.13). 

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
circuitBreaker:
  type: simpleMemory
  memoryThreshold: 20
  circuitCheckIntervalInMillis: 1000
  timeoutInSeconds: 90
```
</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                            | Definition                                                     | Example      |
--------------------------------------|----------------------------------------------------------------|--------------
| spark.openlineage.circuitBreaker.type | Circuit breaker type selected                                  | simpleMemory |
| spark.openlineage.circuitBreaker.memoryThreshold | Memory threshold                                               | 20           |
| spark.openlineage.circuitBreaker.circuitCheckIntervalInMillis | Frequency of checking circuit breaker                          | 1000         |
| spark.openlineage.circuitBreaker.timeoutInSeconds | Optional timeout for OpenLineage execution (Since version 1.13)| 90            |

</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                            | Definition                                  | Example     |
--------------------------------------|---------------------------------------------|-------------
| openlineage.circuitBreaker.type | Circuit breaker type selected               | simpleMemory |
| openlineage.circuitBreaker.memoryThreshold | Memory threshold                            | 20 |
| openlineage.circuitBreaker.circuitCheckIntervalInMillis | Frequency of checking circuit breaker       | 1000        |
| spark.openlineage.circuitBreaker.timeoutInSeconds | Optional timeout for OpenLineage execution (Since version 1.13) | 90            |

</TabItem>
</Tabs>

### Java Runtime Circuit Breaker

More complex version of circuit breaker. The amount of free memory can be low as long as
amount of time spent on Garbage Collection is acceptable. `JavaRuntimeCircuitBreaker` closes
when free memory drops below threshold and amount of time spent on garbage collection exceeds
given threshold (`10%` by default). The circuit breaker is always open when checked for the first time
as GC threshold is computed since the previous circuit breaker call.
`circuitCheckIntervalInMillis` parameter is used
to configure a frequency circuit breaker is called.
Default value is `1000ms`, when no entry in config.
`timeoutInSeconds` is optional. If set, OpenLineage code execution is terminated when a timeout
is reached (added in version 1.13).

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
circuitBreaker:
  type: javaRuntime
  memoryThreshold: 20
  gcCpuThreshold: 10
  circuitCheckIntervalInMillis: 1000
  timeoutInSeconds: 90
```
</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                            | Definition                            | Example     |
--------------------------------------|---------------------------------------|-------------
| spark.openlineage.circuitBreaker.type | Circuit breaker type selected         | javaRuntime |
| spark.openlineage.circuitBreaker.memoryThreshold | Memory threshold                      | 20 |
| spark.openlineage.circuitBreaker.gcCpuThreshold | Garbage Collection CPU threshold      | 10 |
| spark.openlineage.circuitBreaker.circuitCheckIntervalInMillis | Frequency of checking circuit breaker | 1000        |
| spark.openlineage.circuitBreaker.timeoutInSeconds | Optional timeout for OpenLineage execution (Since version 1.13)| 90            |


</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                            | Definition                            | Example     |
--------------------------------------|---------------------------------------|-------------
| openlineage.circuitBreaker.type | Circuit breaker type selected         | javaRuntime |
| openlineage.circuitBreaker.memoryThreshold | Memory threshold                      | 20 |
| openlineage.circuitBreaker.gcCpuThreshold | Garbage Collection CPU threshold      | 10 |
| openlineage.circuitBreaker.circuitCheckIntervalInMillis | Frequency of checking circuit breaker | 1000        |
| spark.openlineage.circuitBreaker.timeoutInSeconds | Optional timeout for OpenLineage execution (Since version 1.13) | 90            |


</TabItem>
</Tabs>

### Custom Circuit Breaker

List of available circuit breakers can be extended with custom one loaded via ServiceLoader
with own implementation of `io.openlineage.client.circuitBreaker.CircuitBreakerBuilder`. 