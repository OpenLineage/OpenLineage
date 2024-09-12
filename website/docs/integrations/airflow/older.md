---
sidebar_position: 2
title: Supported Airflow versions
---

:::caution
This page is about Airflow's external integration that works mainly for Airflow versions <2.7. 
[If you're using Airflow 2.7+, look at native Airflow OpenLineage provider documentation.](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html)  <br /><br /> 

The ongoing development and enhancements will be focused on the `apache-airflow-providers-openlineage` package, 
while the `openlineage-airflow` will primarily be updated for bug fixes.
:::

#### SUPPORTED AIRFLOW VERSIONS

##### Airflow 2.7+

This package **should not** be used starting with Airflow 2.7.0 and **can not** be used with Airflow 2.8+. 
It was designed as Airflow's external integration that works mainly for Airflow versions <2.7.
For Airflow 2.7+ use the native Airflow OpenLineage provider 
[package](https://airflow.apache.org/docs/apache-airflow-providers-openlineage) `apache-airflow-providers-openlineage`.

##### Airflow 2.3 - 2.6

The integration automatically registers itself starting from Airflow 2.3 if it's installed on the Airflow worker's Python.
This means you don't have to do anything besides configuring where the events are sent, which is described in the [configuration](#configuration) section.

##### Airflow 2.1 - 2.2

> **_NOTE:_** The last version of openlineage-airflow to support Airflow versions 2.1-2.2 is **1.14.0**  

<br />

Integration for those versions has limitations: it does not support tracking failed jobs, 
and job starts are registered only when a job ends (a `LineageBackend`-based approach collects all metadata 
for a task on each task's completion).

To make OpenLineage work, in addition to installing `openlineage-airflow` you need to set your `LineageBackend` 
in your [airflow.cfg](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html) or via environmental variable `AIRFLOW__LINEAGE__BACKEND` to

```
openlineage.lineage_backend.OpenLineageBackend
```

The OpenLineageBackend does not take into account manually configured inlets and outlets. 

##### Airflow <2.1 

OpenLineage does not work with versions older than Airflow 2.1.
