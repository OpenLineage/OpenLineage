import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::info
This feature is available in OpenLineage versions >= 1.9.0.
:::

To prevent from over-instrumentation OpenLineage integration provides a circuit breaker mechanism
that stops OpenLineage from creating, serializing and sending OpenLineage events.

### Simple Memory Circuit Breaker

This circuit breaker provides a straightforward protective mechanism by monitoring
a single metric: the amount of free memory in the JVM. It is a lightweight option ideal for 
preventing `OutOfMemoryError` conditions when memory usage is the primary concern.

**Triggering Logic**

The circuit starts in a **closed** (operational) state, allowing OpenLineage events to be collected.
It will **open** (trip and temporarily disable OpenLineage) if the percentage of free
JVM heap memory drops **below** the configured `memoryThreshold`, which is the only condition it checks.

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

| Parameter                            | Definition                                                                                                                  | Example      |
--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|--------------
| spark.openlineage.circuitBreaker.type | Must be set to `simpleMemory` to enable this circuit breaker.                                                               | simpleMemory |
| spark.openlineage.circuitBreaker.memoryThreshold | The minimum percentage of **free** heap memory required. If free memory drops below this value, the circuit will open. Default `20`. | 20           |
| spark.openlineage.circuitBreaker.circuitCheckIntervalInMillis | The frequency, in milliseconds, at which the free memory is checked. Default `1000`. | 1000         |
| spark.openlineage.circuitBreaker.timeoutInSeconds | (Optional) A timeout for any single OpenLineage operation. This applies independently of the memory check. (Since v1.13) | 90            |

</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                                               | Definition                                                                                                                  | Example      |
---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|--------------
| openlineage.circuitBreaker.type                   | Must be set to `simpleMemory` to enable this circuit breaker.                                                               | simpleMemory |
| openlineage.circuitBreaker.memoryThreshold              | The minimum percentage of **free** heap memory required. If free memory drops below this value, the circuit will open. Default `20`. | 20           |
| openlineage.circuitBreaker.circuitCheckIntervalInMillis | The frequency, in milliseconds, at which the free memory is checked. Default `1000`. | 1000         |
| openlineage.circuitBreaker.timeoutInSeconds             | (Optional) A timeout for any single OpenLineage operation. This applies independently of the memory check. (Since v1.13) | 90            |

</TabItem>
</Tabs>

### Java Runtime Circuit Breaker

This circuit breaker provides a sophisticated health check by monitoring two key indicators of
JVM health: free memory and garbage collection (GC) overhead. It is designed to disable OpenLineage
only when the application is both low on memory and actively struggling to reclaim it.

**Triggering Logic**

The circuit starts in a closed (operational) state. It will open (trip and temporarily disable OpenLineage)
only when both of the following conditions are met during a single check:

1. The percentage of free JVM heap memory drops **below** the configured `memoryThreshold`.
2. The percentage of CPU time spent on Garbage Collection since the last check rises **above**
the configured `gcCpuThreshold`.

Because both conditions must be true, it allows the application to handle temporary dips
in free memory as long as the GC process is not overwhelmed.

**Note on Initial State**: The GC overhead is calculated as a percentage of time between checks. 
On the very first check after the application starts, this metric is not yet available.
Therefore, the circuit will remain **closed** (enabled) for the first event, which begins the monitoring cycle.

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

| Parameter                            | Definition                                                                                                                                | Example     |
--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|-------------
| spark.openlineage.circuitBreaker.type | Must be set to `javaRuntime` to enable this specific circuit breaker.                                                                     | javaRuntime |
| spark.openlineage.circuitBreaker.memoryThreshold | The minimum percentage of free heap memory required. The circuit may open if **free** memory drops below this value. Default `20`.        | 20 |
| spark.openlineage.circuitBreaker.gcCpuThreshold | The maximum allowed percentage of CPU time spent on Garbage Collection. The circuit may open if GC time exceeds this value. Default `10`. | 10 |
| spark.openlineage.circuitBreaker.circuitCheckIntervalInMillis | The frequency, in milliseconds, at which the memory and GC thresholds are checked. Default `1000`.                                        | 1000        |
| spark.openlineage.circuitBreaker.timeoutInSeconds | (Optional) A timeout for any single OpenLineage operation. If an emit action takes longer than this, it is terminated. (Since v1.13)      | 90    |


</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                           | Definition                                                                                                                                | Example     |
-------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|-------------
| openlineage.circuitBreaker.type | Must be set to `javaRuntime` to enable this specific circuit breaker.                                                                     | javaRuntime |
| openlineage.circuitBreaker.memoryThreshold | The minimum percentage of free heap memory required. The circuit may open if **free** memory drops below this value. Default `20`.        | 20 |
| openlineage.circuitBreaker.gcCpuThreshold | The maximum allowed percentage of CPU time spent on Garbage Collection. The circuit may open if GC time exceeds this value. Default `10`. | 10 |
| openlineage.circuitBreaker.circuitCheckIntervalInMillis | The frequency, in milliseconds, at which the memory and GC thresholds are checked. Default `1000`.                                        | 1000        |
| openlineage.circuitBreaker.timeoutInSeconds | (Optional) A timeout for any single OpenLineage operation. If an emit action takes longer than this, it is terminated. (Since v1.13)      | 90    |


</TabItem>
</Tabs>

### Custom Circuit Breaker

List of available circuit breakers can be extended with custom one loaded via ServiceLoader
with own implementation of `io.openlineage.client.circuitBreaker.CircuitBreakerBuilder`. 


### Task Queue based Async CircuitBreaker

High-volume Spark applications can generate an excessive number of events,
which can overwhelm the connector and negatively impact the application by choking the shared listener bus.

The `TaskQueueCircuitBreaker` is designed to mitigate this issue.
It manages event processing by adding each task to a bounded queue and handling them asynchronously.
To attempt to preserve event order, it waits a configurable amount of time for a task to complete.
For critical situations, a `close()` method allows for abandoning all pending tasks to immediately unblock the listener bus.

<Tabs groupId="async">
<TabItem value="yaml" label="Yaml Config">

```yaml
circuitBreaker:
  type: asyncTaskQueue
  threadCount: 2
  queueSize: 10
  blockingTimeInSeconds: 1
  shutdownTimeoutSeconds: 60
```
</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                                               | Definition                                                                                                                                   | Example        |
---------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|----------------
| spark.openlineage.circuitBreaker.type                   | Must be set to `asyncTaskQueue` to enable this circuit breaker.                                                                              | asyncTaskQueue |
| spark.openlineage.circuitBreaker.threadCount            | The number of dedicated threads in the fixed-size pool used for processing events. Default `2`.                                              | 2              |
| spark.openlineage.circuitBreaker.queueSize              | The maximum number of events that can be held in the queue awaiting processing. New events are rejected if the queue is full. Default `10`.  | 10             |
| spark.openlineage.circuitBreaker.blockingTimeInSeconds  | Initial blocking time of async call, can be used to improve event ordering. Default `1`.                                                     | 1              |
| spark.openlineage.circuitBreaker.shutdownTimeoutSeconds | The maximum time the system will wait for the queue to drain during a graceful shutdown before abandoning any remaining tasks. Default `60`. | 60             |


</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                                         | Definition                                                                                                                                   | Example        |
---------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|----------------
| openlineage.circuitBreaker.type                   | Must be set to `asyncTaskQueue` to enable this circuit breaker.                                                                              | asyncTaskQueue |
| openlineage.circuitBreaker.threadCount            | The number of dedicated threads in the fixed-size pool used for processing events. Default `2`.                                              | 2              |
| openlineage.circuitBreaker.queueSize              | The maximum number of events that can be held in the queue awaiting processing. New events are rejected if the queue is full. Default `10`.  | 10             |
| openlineage.circuitBreaker.blockingTimeInSeconds  | Initial blocking time of async call, can be used to improve event ordering. Default `1`.                                                     | 1              |
| openlineage.circuitBreaker.shutdownTimeoutSeconds | The maximum time the system will wait for the queue to drain during a graceful shutdown before abandoning any remaining tasks. Default `60`. | 60             |

</TabItem>
</Tabs>