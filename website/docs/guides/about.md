---
sidebar_position: 1
---

# About These Guides

The following tutorials take you through the process of exploiting the lineage metadata provided by Marquez and OpenLineage to solve common data engineering problems and make new analytical and historical insights into your pipelines.

The first tutorial, "Using OpenLineage with Spark," provides an introduction to OpenLineage's integration with Apache Spark. You will learn how to use Marquez and the OpenLineage standard to produce lineage metadata about jobs and datasets created using Spark and BigQuery in a Jupyter notebook environment.

The second tutorial, "Using OpenLineage with Airflow," shows you how to use OpenLineage on Apache Airflow to produce data lineage on supported operators to emit lineage events to Marquez backend. The tutorial also introduces you to the OpenLineage proxy to monitor the event data being emitted.

The third tutorial, "Backfilling Airflow DAGs Using Marquez," shows you how to use Marquez's Airflow integration and the Marquez CLI to backfill failing runs with the help of lineage metadata. You will learn how data lineage can be used to automate the backfilling process.

The fourth tutorial, "Using Marquez with dbt," takes you through the process of setting up Marquez's dbt integration to harvest metadata produced by dbt. You will learn how to create a Marquez instance, install the integration, configure your dbt installation, and test the configuration using dbt.  