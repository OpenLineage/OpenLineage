---
sidebar_position: 5
title: dbt
---

dbt (data build tool) is a powerful transformation engine. It operates on data already within a warehouse, making it easy for data engineers to build complex pipelines from the comfort of their laptops. While it doesn’t perform extraction and loading of data, it’s extremely powerful at transformations.

To learn more about dbt, visit the [documentation site](https://docs.getdbt.com) or run through the [getting started tutorial](https://docs.getdbt.com/tutorial/setting-up).

## How does dbt work with OpenLineage?

Fortunately, dbt already collects a lot of the data required to create and emit OpenLineage events. When it runs, it creates a `target/manifest.json` file containing information about jobs and the datasets they affect, and a `target/run_results.json` file containing information about the run-cycle. These files can be used to trace lineage and job performance. In addition, by using the `create catalog` command, a user can instruct dbt to create a `target/catalog.json` file containing information about dataset schemas.

These files contain everything needed to trace lineage. However, the `target/manifest.json` and `target/run_results.json` files are only populated with comprehensive metadata after completion of a run-cycle. 

This integration is implemented as a wrapper script, `dbt-ol`, that calls `dbt` and, after the run has completed, collects information from the three json files and calls the OpenLineage API accordingly. For most users, enabling OpenLineage metadata collection can be accomplished by simply substituting `dbt-ol` for `dbt` when performing a run.

## Preparing a dbt project for OpenLineage

Right now, `openlineage-dbt` supports only these dbt adapters:

* `bigquery`
* `snowflake`
* `spark` (`thrift` and `odbc`, but not `local`)
* `redshift`
* `athena`
* `glue`
* `postgres`
* `clickhouse`
* `trino`
* `databricks`
* `sqlserver`
* `dremio`
* `duckdb`

First, we need to install the integration:

```bash
pip3 install openlineage-dbt
```

Next, we specify where we want dbt to send OpenLineage events by setting the `OPENLINEAGE_URL` environment variable. For example, to send OpenLineage events to a local instance of Marquez, use:

```bash
OPENLINEAGE_URL=http://localhost:5000
```

Finally, we can optionally specify a namespace where the lineage events will be stored. For example, to use the namespace "dev":

```bash
OPENLINEAGE_NAMESPACE=dev
```

More configuration parameters can be found in [Python client documentation](../client/python.md#configuration)

## Running dbt with OpenLineage

To run your dbt project with OpenLineage collection, simply replace `dbt` with `dbt-ol`:

```bash
dbt-ol run
```

The `dbt-ol` wrapper supports all of the standard `dbt` subcommands, and is safe to use as a substitutuon (i.e., in an alias). Once the run has completed, you will see output containing the number of events sent via the OpenLineage API:

```bash
Completed successfully

Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
Emitted 4 openlineage events
```

## Where can I learn more?

* Watch [a short demonstration of the integration in action](https://youtu.be/7caHXLDKacg)

## Feedback

What did you think of this guide? You can reach out to us on [slack](https://join.slack.com/t/openlineage/shared_invite/zt-3arpql6lg-Nt~hicnDsnDY_GK_LEX06w) and leave us feedback!  