---
sidebar_position: 1
title: Apache Spark
---

## Supported Spark versions

The OpenLineage Spark integration supports a Spark minor version as long as at least
one major cloud or SaaS provider (for example AWS EMR, Databricks, or Google Cloud
Dataproc) supports it. The project may drop support for a version earlier than that
by TSC decision, but never later.

| Spark version | Status | Notes |
| --- | --- | --- |
| 4.x | Supported | |
| 3.5 | Supported | Oldest version available on supported Databricks runtimes (14.3 LTS, until Feb 2027) |
| 3.4 | Supported | AWS EMR 6.12-6.15 extended support until June 30, 2027 |
| 3.3 | Deprecated | Removal planned after June 30, 2027 (end of AWS EMR 6.8-6.11 extended support) |
| 3.2 | Deprecated | Removal planned after June 30, 2027 (end of AWS EMR 6.6/6.7 extended support) |
| 3.1 and older | Not supported | |
| 2.x | Not supported | Removed in OpenLineage 1.37.0 (#3904) |

What "deprecated" means: deprecated versions still receive new integration releases
until the removal date, but new features may not be available on them, and support
ends on the date above regardless of upstream or provider changes.

Dropping support for a Spark version stops new integration releases for that version.
Existing deployments are not affected: older integration artifacts remain compatible
with the OpenLineage specification and continue to emit valid events.

### For contributors

New code in the Spark integration may use APIs available in the oldest supported,
non-deprecated Spark version listed above. If a change requires a newer Spark API,
either gate it per Spark version (see the existing `spark3x` variant modules) or raise
the baseline question in the PR so the TSC can decide whether to move the floor.

This integration employs the `SparkListener` interface through `OpenLineageSparkListener`, offering
a comprehensive monitoring solution. It examines SparkContext-emitted events to extract metadata
associated with jobs and datasets, utilizing the RDD and DataFrame dependency graphs. This method
effectively gathers information from various data sources, including filesystem sources (e.g., S3
and GCS), JDBC backends, and data warehouses such as Redshift and Bigquery.
