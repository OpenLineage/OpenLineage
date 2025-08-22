---
sidebar_position: 1
title: Apache Spark
---

:::info
This integration is known to work with latest Spark versions as well as other Apache Spark 3.*.
Please refer [here](https://github.com/OpenLineage/OpenLineage/tree/main/integration#openlineage-integrations)
for up-to-date information on versions supported.
:::

This integration employs the `SparkListener` interface through `OpenLineageSparkListener`, offering
a comprehensive monitoring solution. It examines SparkContext-emitted events to extract metadata
associated with jobs and datasets, utilizing the RDD and DataFrame dependency graphs. This method
effectively gathers information from various data sources, including filesystem sources (e.g., S3
and GCS), JDBC backends, and data warehouses such as Redshift and Bigquery.
