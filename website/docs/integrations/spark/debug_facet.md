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

## Debug Facet's content

`DebugFacet` contains following information:

* Classpath information: Spark version, OpenLineage connector version, Scala version, jars added through Spark config as well additional information about classes on the classpath which seem highly relevant for debugging: is Iceberg on the classpath, is BigQuery connector on the classpath, is Delta on the classpath, etc. 
* Information about the system like: Spark deployment mode, Java version, Java vendor, OS name, OS version, timezone.
* Metrics, which apart from being sent to Metric backend, can be filled within DebugFacet at the same time.
* Shortened information about the LogicalPlan which contains tree structure as well class names of the nodes.
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