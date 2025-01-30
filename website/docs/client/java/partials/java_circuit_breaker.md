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


### Task Queue based Async CircuitBreaker
In some cases spark applications generate too many events and processing all those events by the
connector may have adverse effect on the spark application itself, e.g.,
choking the listener bus and making other listeners sharing the listener bus not able to catch up. 
TaskQueueCircuitBreaker offers the functionality to process as many events as possible in such cases, 
while minimizing impact on the spark job. First it queues any task (processing of events) in a bounded queue
and strictly process them asynchronously, while waiting a configurable 
amount of time for the task to complete to make some effort towards
preserving order. Second, it offers a close method to abandon pending tasks and unblock the listeners sharing the same listener bus.
The existing ExecutorCircuitBreaker, though looks similar, is not fully adequate for this need because  
it has a cachedthreadpool, which can result in creation of too many threads and high memory footprint. 
It also rejects a task right away if there's no thread to pick up.

<Tabs groupId="async">
<TabItem value="flink" label="Flink Config">

| Parameter                            | Definition                                                                                                 | Example        |
--------------------------------------|------------------------------------------------------------------------------------------------------------|----------------
| openlineage.circuitBreaker.type | Circuit breaker type selected                                                                              | asyncTaskQueue |
| openlineage.circuitBreaker.threadCount | Num threads to process task                                                                                | 2              |
| openlineage.circuitBreaker.queueSize | The size of task queue                                                                                     | 1000           |
| openlineage.circuitBreaker.circuitCheckIntervalInMillis | Frequency of checking circuit breaker                                                                      | 1000           |
| spark.openlineage.circuitBreaker.timeoutInSeconds | Optional timeout for OpenLineage execution (Since version 1.13)                                            | 3              |
| spark.openlineage.circuitBreaker.shutdownTimeoutSeconds | The duration through which the circuit breaker waits on close to wait for the queued tasks to be processed | 100            |



</TabItem>
</Tabs>