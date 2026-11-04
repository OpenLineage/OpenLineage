---
sidebar_position: 1
---

# About

The following tutorials guide you through using OpenLineage to collect, emit, and leverage data lineage metadata across your pipelines to solve common data engineering problems and gain analytical and operational insights.

While several tutorials use [Marquez](https://marquezproject.ai/) as an open-source reference implementation for backend storage and visualization, OpenLineage is designed to be consumer- and vendor-neutral. Emitted OpenLineage events can be sent to and consumed by any OpenLineage-compatible backend or metadata platform.

- **[Using OpenLineage with Spark](spark.md)** provides an introduction to OpenLineage's integration with Apache Spark. You will learn how to produce lineage metadata about jobs and datasets created using Spark and BigQuery in a Jupyter notebook environment and emit events to a backend.
- **[Using OpenLineage with Airflow](airflow-quickstart.md)** demonstrates how to use OpenLineage on Apache Airflow to capture data lineage from supported operators and emit events to a backend. The tutorial also introduces the OpenLineage proxy to inspect and monitor emitted event data.
- **[Backfilling Airflow DAGs Using Marquez](airflow-backfill-dags.md)** shows how lineage metadata stored in an OpenLineage-compatible backend can be used alongside the Marquez CLI to automate backfilling for failing DAG runs.
- **[Using Marquez with dbt](dbt.md)** takes you through configuring dbt with OpenLineage to harvest lineage metadata produced during dbt runs and view it in a backend.
- **[OpenLineage for Spark Connectors](spark-connector.md)** explores how OpenLineage extracts lineage from Spark LogicalPlans and provides architectural guidance for Spark connector developers.
