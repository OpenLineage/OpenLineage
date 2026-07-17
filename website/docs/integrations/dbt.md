---
sidebar_position: 6
title: dbt
---

dbt (data build tool) is a powerful transformation engine. It operates on data already within a warehouse, making it easy for data engineers to build complex pipelines from the comfort of their laptops. While it doesn’t perform extraction and loading of data, it’s extremely powerful at transformations.

To learn more about dbt, visit the [documentation site](https://docs.getdbt.com) or run through the [getting started tutorial](https://docs.getdbt.com/tutorial/setting-up).

## How does dbt work with OpenLineage?

dbt generates rich metadata that OpenLineage uses to trace datasets, jobs, and lineage. The OpenLineage dbt integration has evolved to support three main approaches for collecting lineage, each with different capabilities and trade-offs:

1. **dbt CLI Wrapper (`dbt-ol`)**
2. **Artifact Processor**
3. **Structured Log Processor**

---

### Ingestion Approaches Comparison

| Feature | dbt CLI Wrapper (`dbt-ol`) | Artifact Processor | Structured Log Processor |
| :--- | :--- | :--- | :--- |
| **Ingestion Type** | Post-Run | Post-Run | Real-Time (Streaming) |
| **Telemetry Source** | Target JSON files | Target JSON files | JSON Log Stream |
| **Hierarchy Level** | Node (Models/Tests) | Node (Models/Tests) | Command → Node → Query |
| **Best For** | Standalone CLI runs, simple cron | Airflow Operators (e.g., Cosmos) | Real-time dashboards, long-running jobs |

---

### 1. dbt CLI Wrapper (`dbt-ol`)

This is the standard approach for standalone dbt execution.

#### How it Works
The `dbt-ol` command acts as a wrapper around the standard `dbt` executable:
1. It executes the dbt command you provide (e.g., `dbt-ol run`).
2. Once the execution completes, the wrapper reads dbt’s generated JSON artifacts from the `target/` directory:
   * `manifest.json` (the project's dependency graph)
   * `run_results.json` (execution outcomes, timing, and statuses)
   * `catalog.json` (schema and column metadata, if generated)
3. It parses the metadata, translates it to OpenLineage schemas, and emits the events to the configured `OPENLINEAGE_URL`.

#### Preparing a dbt project for OpenLineage

Right now, `openlineage-dbt` supports these dbt adapters:

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
* `fabric`
* `dremio`
* `duckdb`

First, install the integration:

```bash
pip3 install openlineage-dbt
```

Next, specify where you want dbt to send OpenLineage events by setting the `OPENLINEAGE_URL` environment variable. For example, to send OpenLineage events to a local instance of Marquez, use:

```bash
OPENLINEAGE_URL=http://localhost:5000
```

Finally, you can optionally specify a namespace where the lineage events will be stored. For example, to use the namespace "dev":

```bash
OPENLINEAGE_NAMESPACE=dev
```

You can also override the job name sent by dbt OpenLineage events by providing env variable:
```bash
OPENLINEAGE_DBT_JOB_NAME=<your-job-name>
```
or passing `--openlineage-dbt-job-name <your-job-name>` in the dbt command line.

More configuration parameters can be found in the [Python client documentation](../client/python/configuration.md).

#### Running dbt with OpenLineage

To run your dbt project with OpenLineage collection, simply replace `dbt` with `dbt-ol`:

```bash
dbt-ol run
```

The `dbt-ol` wrapper supports all of the standard `dbt` subcommands, and is safe to use as a substitution (i.e., in an alias). Once the run has completed, you will see output containing the number of events sent via the OpenLineage API:

```bash
Completed successfully

Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
Emitted 4 openlineage events
```

---

### 2. Artifact Processor

The Artifact Processor is a library-level integration used by orchestrators to extract lineage post-run.

#### How it Works
Instead of running a CLI wrapper, an orchestrator (such as Apache Airflow using operators like [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/)) runs standard dbt commands. Once the run completes, the orchestrator triggers the OpenLineage `DbtLocalArtifactProcessor` library directly:
1. The processor reads the generated `manifest.json`, `run_results.json`, and `catalog.json` files from the specified target directory.
2. It parses the files and constructs/emits the OpenLineage events.

#### Relationship to the Wrapper
The `dbt-ol` wrapper actually uses the same underlying artifact parsing logic as the Artifact Processor. The key difference is that the wrapper executes the dbt command and handles the file paths automatically, while the Artifact Processor is designed to be called programmatically by orchestrators that manage their own file locations and execution lifecycles.

---

### 3. Structured Log Processor

The Structured Log Processor is a real-time integration method that parses JSON logs streamingly.

#### How it Works
Starting with dbt Core v1.x, dbt supports structured logging, emitting execution events as JSON lines.
1. The integration listens to the structured log stream output by dbt (via standard output or log files).
2. It parses the log events (such as node execution start, query start, node completion) as they occur.
3. OpenLineage events are emitted **in real-time** while the dbt run is actively executing.

#### Capabilities & Benefits
* **Real-time streaming**: Events are emitted as models finish, providing instant observability without waiting for the whole run to complete.
* **Richer Event Hierarchies**: Supports a detailed `command → node → SQL query` execution hierarchy.
* **Query-level Fidelity**: Can observe specific SQL query executions that are omitted or lost in the final post-run artifacts.

---

## Where can I learn more?

* Watch [a short demonstration of the integration in action](https://youtu.be/7caHXLDKacg)

## Feedback

What did you think of this guide? You can reach out to us on [slack](https://join.slack.com/t/openlineage/shared_invite/zt-3arpql6lg-Nt~hicnDsnDY_GK_LEX06w) and leave us feedback!
