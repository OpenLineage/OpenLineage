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

---

## Core Parsing Mechanisms

### 1. Artifact Processor (Post-Run Parsing)

The Artifact Processor extracts lineage after a dbt run completes by parsing dbt's generated JSON artifact files.

#### How it Works
1. When dbt finishes, the processor reads three target JSON files from the `target/` directory:
   * `manifest.json`: Contains the complete dependency graph, compiled SQL queries, and node definitions.
   * `run_results.json`: Contains execution results, execution status, node timing, and query IDs.
   * `catalog.json` *(optional)*: Contains database schema information, column data types, and table statistics.
2. The processor converts the node metadata into OpenLineage dataset and job definitions. Although the Artifact Processor emits flat node-level events (a separate event pair for each model or test), it links each node event to an orchestrator's parent run (such as an Airflow DAG or Cosmos task) by appending a `parent` run facet (`ParentRunFacet`) when parent metadata is provided.

#### Event Emission Model
* **Events per Command**: The Artifact Processor emits a pair of events (**START** and **COMPLETE** or **FAIL**) for every executed dbt node (model, seed, snapshot, or test). 
* For example, if a `dbt run` executes 5 models, the Artifact Processor will emit 10 OpenLineage events (5 `START` events followed by 5 `COMPLETE`/`FAIL` events).

#### OpenLineage Facets Emitted
The Artifact Processor enriches OpenLineage events with rich dbt-specific and standard facets:

* **Always Present (Core Facets)**:
  * **Job Facet — `jobType`**: Identifies the job type (`jobType="JOB"`, `processingType="BATCH"`, `integration="DBT"`).
  * **Job Facet — `dbt_node_metadata`**: Contains node details including `unique_id`, `resource_type`, `materialization`, `original_file_path`, and `tags`.
  * **Run Facet — `dbt_version`**: Contains the dbt core version and active database adapter name.
  * **Run Facet — `dbt_run`**: Contains run-wide execution metadata (e.g. `invocation_id`, `project_name`, `profile_name`, `full_refresh`).
  * **Dataset Facet — `symlink_identifiers`**: Contains the database, schema, and table/view names for input and output datasets.
  * **Dataset Facet — `documentation`**: Contains model-level and dataset descriptions from dbt project documentation.
* **Optional Facets**:
  * **Run Facet — `parent`**: Identifies the orchestrator's parent run (`ParentRunFacet`) when parent context is provided.
  * **Dataset Facet — `schema`**: Detailed column names and data types (emitted when `catalog.json` is available).
  * **Dataset Facet — `dbt_model`**: Detailed model configuration (e.g. `materialized`, `owner`, `incremental` strategy).
  * **Job Facet — `sql`**: Compiled SQL source code for the model or test.
  * **Dataset Facet — `columnLineage`**: Fine-grained column-level input/output mapping (when column-level lineage parsing is enabled).
  * **Dataset Facet — `dbt_exposures`**: Metadata for downstream dbt exposures.

> ℹ️ **Code Reference Disclaimer**: The OpenLineage dbt integration evolves rapidly as dbt and OpenLineage add new features. The authoritative source for supported facets and schemas is the source code in [`facets.py`](https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/src/openlineage/common/provider/dbt/facets.py).

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

1. **dbt Command Run**: An overall parent event representing the complete `dbt` invocation (e.g. `dbt run`). Parent run context passed from an external orchestrator is attached to this top-level command run.
2. **Node Runs**: Nested child events for each model or test execution, linked to the main dbt command parent run.
3. **Query Executions**: Individual SQL query execution events nested under their respective node runs.

```text
Orchestrator Parent Run (Airflow / Cosmos)
 └── dbt Command Run (Parent)
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

## Passing Parent Context from External Orchestrators

Whether using the `dbt-ol` CLI wrapper, `DbtLocalArtifactProcessor`, or `DbtStructuredLogsProcessor`, you can link the dbt execution to a parent orchestrator run (such as an Airflow DAG or Cosmos task):

### 1. Via Environment Variables
Set the standardized `OPENLINEAGE_CONTEXT` environment variable (a JSON payload formatted with `parent` and optional `root` keys):

```bash
export OPENLINEAGE_CONTEXT='{
  "parent": {
    "run": {"runId": "f99310b4-3c3c-1a1a-2b2b-c1b95c24ff11"},
    "job": {"namespace": "airflow-namespace", "name": "airflow-dag.dbt_task"}
  }
}'
```

Alternatively, use the legacy `OPENLINEAGE_PARENT_ID` format:
```bash
export OPENLINEAGE_PARENT_ID="airflow-namespace/airflow-dag.dbt_task/f99310b4-3c3c-1a1a-2b2b-c1b95c24ff11"
```

### 2. Via Programmatic APIs
When invoking the Python processors directly, instantiate and pass a `ParentRunMetadata` object:

```python
from openlineage.common.provider.dbt import DbtLocalArtifactProcessor, ParentRunMetadata

parent_metadata = ParentRunMetadata(
    run_id="f99310b4-3c3c-1a1a-2b2b-c1b95c24ff11",
    job_name="airflow-dag.dbt_task",
    job_namespace="airflow-namespace"
)

processor = DbtLocalArtifactProcessor(
    dbt_run_metadata=parent_metadata,
    project_dir="./dbt_project",
    target_path="./target"
)
```

---

## dbt-Specific Custom Facets

OpenLineage defines custom facets specifically for dbt metadata. Below are the custom facets attached to OpenLineage jobs, runs, and datasets:

### 1. `dbt_node_metadata` (`DbtNodeJobFacet`)
Attached to node jobs (models, tests, seeds, snapshots) to capture node properties defined in the dbt manifest.

```json
{
  "dbt_node_metadata": {
    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/dbt-node-job-facet.json",
    "unique_id": "model.jaffle_shop.stg_customers",
    "database": "analytics",
    "schema": "staging",
    "alias": "stg_customers",
    "original_file_path": "models/staging/stg_customers.sql"
  }
}
```

### 2. `dbt_version` (`DbtVersionRunFacet`)
Attached to runs to record the dbt core version.

```json
{
  "dbt_version": {
    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/dbt-version-run-facet.json",
    "version": "1.8.0"
  }
}
```

### 3. `dbt_run` (`DbtRunRunFacet`)
Attached to runs to capture invocation metadata.

```json
{
  "dbt_run": {
    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/dbt-run-run-facet.json",
    "invocation_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "project_name": "jaffle_shop",
    "profile_name": "default",
    "full_refresh": false
  }
}
```

### 4. `dbt_model` (`DbtModelDatasetFacet`)
Attached to output datasets to record the model's resolved configuration (materialization, owner, and incremental strategies).

```json
{
  "dbt_model": {
    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/dbt-model-dataset-facet.json",
    "config": {
      "materialized": "incremental",
      "owner": "data-team",
      "incremental": {
        "strategy": "merge",
        "unique_key": ["customer_id"]
      }
    }
  }
}
```

### 5. `dbt_exposures` (`DbtExposuresDatasetFacet`)
Attached to model output datasets listing downstream dbt exposures (dashboards, notebooks, etc.).

```json
{
  "dbt_exposures": {
    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/dbt-exposures-dataset-facet.json",
    "exposures": [
      {
        "unique_id": "exposure.jaffle_shop.executive_dashboard",
        "name": "executive_dashboard",
        "type": "dashboard",
        "url": "https://bi.company.com/dashboards/123"
      }
    ]
  }
}
```

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
