---
sidebar_position: 8
title: Debug Facet
---

Whenever OpenLineage event is properly emitted, but its content is not as expected, debug facet is the easiest way to start
with and collect more insights about the problem.

:::info
As a name suggests, debug facet is not meant to be used in production by default. 
However, it definitely makes sense to enable it ad-hoc when needed.
:::

:::info
`DebugFacet` is turned off by default. To enable it, set the following configuration has to be applied:
```yaml
spark.openlineage.facets.debug.disabled=false
```
:::

## Facet content

`DebugFacet` contains following information:

* Classpath information: Spark version, OpenLineage connector version, Scala version, jars added through Spark config as well additional information about classes on the classpath which seem highly relevant for debugging: is Iceberg on the classpath, is BigQuery connector on the classpath, is Delta on the classpath, etc. 
* Information about the system like: Spark deployment mode, Java version, Java vendor, OS name, OS version, timezone.
* Metrics, which apart from being sent to Metric backend, can be filled within DebugFacet at the same time.
* Shortened information about the LogicalPlan which contains tree structure as well class names of the nodes.

Please refer to `io.openlineage.spark.agent.facets.DebugRunFacet` source code to get more up-to-date information about the fields.