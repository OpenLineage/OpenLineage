---
title: Spark Integration Metrics
sidebar_position: 10
---

# Spark Integration Metrics

The OpenLineage integration with Spark not only utilizes the Java client's metrics but also introduces its own set of metrics specific to Spark operations. Below is a list of these metrics.

## Metrics Overview

The following table provides the metrics added by the Spark integration, along with their definitions and types:

| Metric                                           | Definition                                                             | Type    |
|--------------------------------------------------|------------------------------------------------------------------------|---------|
| `openlineage.spark.event.sql.start`              | Number of SparkListenerSQLExecutionStart events received               | Counter |
| `openlineage.spark.event.sql.end`                | Number of SparkListenerSQLExecutionEnd events received                 | Counter |
| `openlineage.spark.event.job.start`              | Number of SparkListenerJobStart events received                        | Counter |
| `openlineage.spark.event.job.end`                | Number of SparkListenerJobEnd events received                          | Counter |
| `openlineage.spark.event.app.start`              | Number of SparkListenerApplicationStart events received                | Counter |
| `openlineage.spark.event.app.end`                | Number of SparkListenerApplicationEnd events received                  | Counter |
| `openlineage.spark.event.app.start.memoryusage`  | Percentage of used memory at the start of the application              | Counter |
| `openlineage.spark.event.app.end.memoryusage`    | Percentage of used memory at the end of the application                | Counter |
| `openlineage.spark.unknownFacet.time`            | Time spent building the UnknownEntryRunFacet                           | Timer   |
| `openlineage.spark.dataset.input.execution.time` | Time spent constructing input datasets for execution                   | Timer   |
| `openlineage.spark.facets.job.execution.time`    | Time spent building job-specific facets                                | Timer   |
| `openlineage.spark.facets.run.execution.time`    | Time spent constructing run-specific facets                            | Timer   |

