---
sidebar_position: 6
title: dbt
---

dbt (data build tool) is a powerful transformation engine. It operates on data already within a warehouse, making it easy for data engineers to build complex pipelines from the comfort of their laptops. While it doesn’t perform extraction and loading of data, it’s extremely powerful at transformations.

To learn more about dbt, visit the [documentation site](https://docs.getdbt.com) or run through the [getting started tutorial](https://docs.getdbt.com/tutorial/setting-up).

## How does dbt work with OpenLineage?

dbt generates rich telemetry and metadata that OpenLineage uses to trace datasets, jobs, and lineage.

OpenLineage processes dbt telemetry using **two primary parsing mechanisms** based on *when* and *how* metadata is collected:

1. **Artifact Processor (Post-Run)**: Extracts lineage after dbt finishes by parsing generated JSON artifacts (`manifest.json`, `run_results.json`, and optionally `catalog.json`).
2. **Structured Log Processor (Real-Time)**: Extracts lineage while dbt runs by consuming dbt's structured JSON log stream in real time.

### Ingestion Approaches & Parsing Modes Comparison

| Feature / Dimension | Artifact Processor Mode | Structured Log Processor Mode |
| :--- | :--- | :--- |
| **Parsing Mechanism** | Post-Run (Parses `manifest.json`, `run_results.json`, `catalog.json`) | Real-Time (Streams & parses JSON log lines as dbt runs) |
| **Telemetry Source** | Target JSON artifact files | Standard Output / JSON Log Stream |
| **Event Hierarchy** | Flat node events (`START`, `COMPLETE`/`FAIL` per node) | Nested hierarchy (`Command → Node → Query`) |
| **Query Capture** | Retains only the last query ID per node (from `run_results.json`) | Captures all sequential SQL queries executed by a node |
| **Schema & Catalog** | Full schema & column data types when `catalog.json` exists | Basic metadata from execution logs |
| **Key Advantage** | High schema fidelity; simple post-run execution | Instant real-time observability; full multi-query visibility |
| **Assumptions / Trade-offs** | Requires dbt command to finish before emitting lineage | Assumes query log events arrive sequentially in stdout |
| **Execution Options** | `dbt-ol run` (CLI default) or `DbtLocalArtifactProcessor` (Cosmos/Airflow) | `dbt-ol run --consume-structured-logs` |

---

## Core Parsing Mechanisms

### 1. Artifact Processor (Post-Run Parsing)

The Artifact Processor extracts lineage after a dbt run completes by parsing dbt's generated JSON artifact files.

#### How it Works
1. When dbt finishes, the processor reads three target JSON files from the `target/` directory:
   * `manifest.json`: Contains the complete dependency graph, compiled SQL queries, and node definitions.
   * `run_results.json`: Contains execution results, execution status, node timing, and query IDs.
   * `catalog.json` *(optional)*: Contains database schema information, column data types, and table statistics.
2. The processor converts the node metadata into OpenLineage dataset and job definitions, linking parent dependencies to child models.

#### Event Emission Model
* **Events per Command**: The Artifact Processor emits a pair of events (**START** and **COMPLETE** or **FAIL**) for every executed dbt node (model, seed, snapshot, or test). 
* For example, if a `dbt run` executes 5 models, the Artifact Processor will emit 10 OpenLineage events (5 `START` events followed by 5 `COMPLETE`/`FAIL` events).

#### OpenLineage Facets Emitted
The Artifact Processor enriches OpenLineage events with rich dbt-specific and standard facets:

* **Always Present (Core Facets)**:
  * **Job Facet — `dbt_node_metadata`**: Contains node details including `unique_id`, `resource_type`, `materialization`, `original_file_path`, and `tags`.
  * **Run Facet — `dbt_version`**: Contains the dbt core version and active database adapter name.
  * **Dataset Facet — `symlink_identifiers`**: Contains the database, schema, and table/view names for input and output datasets.
  * **Dataset Facet — `documentation`**: Contains model-level and dataset descriptions from dbt project documentation.
* **Optional Facets**:
  * **Dataset Facet — `schema`**: Detailed column names and data types (emitted when `catalog.json` is available).
  * **Job Facet — `sql`**: Compiled SQL source code for the model or test.
  * **Dataset Facet — `columnLineage`**: Fine-grained column-level input/output mapping (when column-level lineage parsing is enabled).
  * **Job Facet — `dbt_exposures`**: Metadata for downstream dbt exposures.

#### Programmatic & Orchestrator Usage
Orchestrators like Apache Airflow (e.g., using [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/)) invoke the `DbtLocalArtifactProcessor` library directly after dbt task completion to parse artifacts without requiring CLI wrappers.

---

### 2. Structured Log Processor (Real-Time Streaming)

The Structured Log Processor is a real-time integration method that parses dbt's JSON log stream while the dbt process executes.

#### How it Works
Starting with dbt Core v1.x, dbt emits structured JSON log events (JSON lines) during execution.
1. The integration listens to dbt's log stream (either from stdout or log files).
2. As log events occur (such as `MainReportVersion`, `NodeStart`, `SQLQuery`, `NodeFinished`), the processor parses them on the fly.
3. OpenLineage events are emitted **in real-time** while the dbt run is actively executing.

#### Event Hierarchy & Structural Differences
Unlike the Artifact Processor which produces flat node-level events after execution, the Structured Log Processor constructs a **nested execution hierarchy**:

1. **dbt Command Run**: An overall parent event representing the complete `dbt` invocation (e.g. `dbt run`).
2. **Node Runs**: Nested child events for each model or test execution, linked to the main dbt command parent run.
3. **Query Executions**: Individual SQL query execution events nested under their respective node runs.

```text
dbt Command Run (Parent)
 └── Node Run: model_a (Child)
      ├── Query Run: CREATE TEMP TABLE... (Grandchild)
      └── Query Run: INSERT INTO model_a... (Grandchild)
 └── Node Run: model_b (Child)
```

#### Query Capture & Multi-Query Attribution
* **Multi-Query Capture**: If a single dbt model executes multiple SQL statements (e.g., pre-hooks, temporary table creation, main model transformation, and post-hooks):
  * **Artifact Processor**: `run_results.json` only retains the last adapter response / query ID for a node, dropping earlier queries.
  * **Structured Log Processor**: Captures every individual SQL query event emitted by dbt as it executes.
* **Sequential Log Attribution Assumption**: The Structured Log Processor attributes SQL queries to nodes under the assumption that query log events arrive **sequentially**. It assigns each captured query ID to the currently active model node based on the stream event order.

---

## Using the dbt CLI Wrapper (`dbt-ol`)

The `dbt-ol` CLI command is a 1:1 drop-in replacement for the standard `dbt` command. It executes your standard `dbt` subcommands and automatically handles OpenLineage event generation and submission.

### Execution Modes in `dbt-ol`

* **Artifact Mode (Default)**: Executes standard `dbt` and parses target artifacts post-run:
  ```bash
  dbt-ol run
  ```
* **Structured Log Mode**: Streams JSON logs and emits events in real-time as models run:
  ```bash
  dbt-ol run --consume-structured-logs
  ```

### Supported dbt Adapters

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

### Installation & Configuration

First, install the integration:

```bash
pip3 install openlineage-dbt
```

Next, set the `OPENLINEAGE_URL` environment variable:

```bash
OPENLINEAGE_URL=http://localhost:5000
```

Optionally, set the namespace:

```bash
OPENLINEAGE_NAMESPACE=dev
```

You can also override the job name sent by dbt OpenLineage events by setting the environment variable:
```bash
OPENLINEAGE_DBT_JOB_NAME=<your-job-name>
```
or by passing `--openlineage-dbt-job-name <your-job-name>` on the command line.

More configuration parameters can be found in [Python client documentation](../client/python/configuration.md).

---

## Where can I learn more?

* Watch [a short demonstration of the integration in action](https://youtu.be/7caHXLDKacg)

## Feedback

What did you think of this guide? You can reach out to us on [slack](https://join.slack.com/t/openlineage/shared_invite/zt-3arpql6lg-Nt~hicnDsnDY_GK_LEX06w) and leave us feedback!
