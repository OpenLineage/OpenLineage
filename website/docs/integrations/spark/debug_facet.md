---
sidebar_position: 8
title: Debugging with Debug Facet
---

Whenever OpenLineage event is properly emitted, but its content is not as expected, debug facet is the easiest way to start
with and collect more insights about the problem.

:::info
As a name suggests, debug facet is not meant to be used in production by default. 
However, it definitely makes sense to enable it ad-hoc when needed, or allow smart debug facet feature to turn it on automatically
when it detects that OpenLineage event is not emitted properly.
:::

## Trace dataset dispatch

To see which dataset builders and plan visitors are checked and invoked, enable
`TRACE` for `io.openlineage.spark.agent.util.DatasetDispatchTrace` in the driver's
logging configuration. You do not need to enable the debug facet.

For Log4j 2, add these entries to `log4j2.properties`:

```properties
logger.datasetDispatch.name = io.openlineage.spark.agent.util.DatasetDispatchTrace
logger.datasetDispatch.level = trace
```

For Spark runtimes using Log4j 1, add this entry to `log4j.properties`:

```properties
log4j.logger.io.openlineage.spark.agent.util.DatasetDispatchTrace=TRACE
```

The selected appender must also accept `TRACE` events. Apply the logging
configuration before starting the Spark application, and disable tracing after
the investigation.

Each input or output extraction phase gets a unique `capture` ID. Trace lines
include the OpenLineage `run` ID, Spark event class, phase, handler class,
scope-local node ID, and invocation ID. `parent` links nested checks and
applications to the invocation that caused them. Visitor adapters report the
underlying builder class.

For example, an application can produce these abbreviated records:

```text
id=2 parent=0 operation=apply handler=...LogicalRelationDatasetBuilder node=1 nodeType=...LogicalRelation
id=2 resultCount=1 durationNanos=42000
id=3 parent=0 operation=check handler=...AnotherBuilder node=1 nodeType=...LogicalRelation
id=3 node=1 visited=already-visited visitedBy=2
id=3 match=false durationNanos=5000
```

`resultCount=0`, `result=null`, and `error=<exception class>` distinguish empty
results, null returns, and recovered failures. A visited-state rejection names
its originating invocation when that update was observed in the same scope;
otherwise, it reports `visitedBy=outside-scope`. An arbitrary predicate returning
`false` does not provide an additional reason.

Tracing observes existing calls without rerunning predicates or changing handler
order. It covers merged plan visitors, safe builder calls, and nested delegation
through those paths. It does not explain dataset reduction, final facet
provenance, or arbitrary extension code that bypasses dispatch.

Each phase is limited to 1,000 records plus a `truncated=true` notice. Records do
not include plan contents, dataset values, or exception messages. Node identity
bookkeeping is bounded and released when the phase finishes, including on
failure. Input and output phases use separate scopes, including when extraction
runs on timeout executor threads.

## Debug Facet's content

`DebugFacet` contains following information:

* Classpath information: Spark version, OpenLineage connector version, Scala version, jars added through Spark config as well additional information about classes on the classpath which seem highly relevant for debugging: is Iceberg on the classpath, is BigQuery connector on the classpath, is Delta on the classpath, etc. 
* Information about the system like: Spark deployment mode, Java version, Java vendor, OS name, OS version, timezone.
* Metrics, which apart from being sent to Metric backend, can be filled within DebugFacet at the same time.
* Shortened information about the LogicalPlan which contains tree structure as well class names of the nodes.
* Memory information including Spark's driver memory configuration and memory usage (free and total memory).
* Logs: logs relating to OpenLineage Spark integration, which can be useful for debugging purposes.

Please refer to `io.openlineage.spark.agent.facets.DebugRunFacet` source code to get more up-to-date information about the fields.

### Debug facet configuration

`DebugFacet` is turned off by default. To enable it, set the following configuration has to be applied:
```yaml
spark.openlineage.facets.debug.disabled=false
```

Additionally, following configuration entries are applicable:

* `spark.openlineage.debug.smart=true` - Enables smart debug facet feature, which automatically turns on debug facet when OpenLineage event is not emitted properly. Disabled by default. For smart debug, the debug facet will be emitted only on `COMPLETE` when criteria depending on `smartMode` are met.
* `spark.openlineage.debug.smartMode` - can be either `output-missing` to activate debug facet when outputs are missing or `any-missing` to activate when inputs or outputs are missing. Defaults to `any-missing`.
* `spark.openlineage.debug.metricsDisabled` - By default Spark integration metrics are included in the debug facet. This can be useful for debugging how much time has the integration spent on each dataset builder. The representation of the metrics with tags within a JSON document can result in increased payload size, so it can be disabled by setting this configuration to `true`.
* `spark.openlineage.debug.payloadSizeLimitInKilobytes=50` - Maximal size of the debug facet payload in kilobytes of JSON. If the payload exceeds this limit, it debug facet will contain only a single log message with the information about the exceeded size. Defaults to 100 kilobytes.

### Debug facet with fine-grained timeouts

OpenLineage allows circuit breakers which timeout lineage code execution when it takes too long. 
Additional configuration options allow incomplete OpenLineage events to be emitted with debug facet, when the circuit breaker is triggered:
```yaml
spark.openlineage.timeout.buildDatasetsTimePercentage=60
spark.openlineage.timeout.facetsBuildingTimePercentage=80
```
These options define the percentage of the total timeout time that can be spent on building datasets facets or all facets (job, run and datasets facets) respectively.
The settings are applied only when circuit breaker with timeout is configured. `TimeoutCircuitBreaker` is the simplest to turn this on.

OpenLineage code flows through:
 * job facets building, 
 * input datasets building,
 * output datasets building,
 * run facets building,
 * event serialization and sending.

Given an example circuit breaker with a timeout of 30 seconds, and `buildDatasetsTimePercentage=60` and `facetsBuildingTimePercentage=80`, the following timeouts will be applied:
 * Dataset generation should accomplish within 18 seconds (60% of 30 seconds). If this fails, there are still 12 seconds left for job and run facets building as well as event serialization and sending.
 * All facets building should accomplish within 24 seconds (80% of 30 seconds). If this fails, there are still 6 seconds left for emitting event with facets already included.
 * In case of timeout, `DebugRunFacet` is included with a log entry added mentioning that the event is incomplete due to the timeout.

When OpenLineage event is not emitted properly, debug facet can be emitted as a part of incomplete event. In this case, the debug facet will contain only the information about the classpath, system information and logs. The rest of the fields will be empty.