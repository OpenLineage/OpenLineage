# OpenLineage dbt integration

Wrapper script for automatic metadata collection from dbt

## Features

**Metadata**

* Model run lifecycle
* Model inputs / outputs

## Requirements

- [Python >= 3.8](https://www.python.org/downloads)
- [dbt >= 0.20](https://www.getdbt.com/)

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
* `presto`
* `watsonx_presto` (IBM watsonx.data Presto)
* `databricks`
* `sqlserver`
* `fabric` (Microsoft Fabric Warehouse)
* `dremio`
* `duckdb`
* `polars` ([dbt-polars](https://github.com/datamindedbe/dbt-polars), Iceberg REST and SQL catalogs only, see [dbt-polars](#dbt-polars))

## Installation

```bash
$ pip3 install openlineage-dbt
```

`openlineage-dbt` doesn't install dbt. It runs the `dbt` executable it finds on `PATH`, so install dbt the way
you normally would, for example an adapter package for dbt Core v1:

```bash
$ pip3 install dbt-snowflake openlineage-dbt
```

To install from source, run:

```bash
$ pip install .
```

## Configuration

### Transport

`openlineage-dbt` uses the OpenLineage Python client to push data to the OpenLineage backend, so any way of configuring Python client will work here as well:

```ini
OPENLINEAGE_URL=http://localhost:5000
OPENLINEAGE_API_KEY=abc
```

dbt integration-specific environment variables:

* `OPENLINEAGE_NAMESPACE` - set if you are using something other than the `default` namespace for job namespace.

### Project source location

When the Python client's `facets.source_code_location.disabled` setting is `false`,
`dbt-ol` includes the selected project's repository-relative directory in
`sourceCodeLocation.path`. For example, `--project-dir teams/finance` emits
`teams/finance` when launched from the repository root; a project at the repository
root emits an empty path. The same applies to artifact, structured-log, and Core v2 modes.

Discovery uses the selected project directory's Git checkout rather than assuming
the wrapper's current directory is the project. Project selection follows `--project-dir`,
the installed dbt engine's project-directory environment variables, then the nearest
`dbt_project.yml` in the current directory or its parents.
If the checkout or repository URL cannot be identified, no project path is inferred. Existing source-location facets are preserved.

### Logging

In addition to conventional logging approaches, the OpenLineage dbt wrapper script provides an alternative way of configuring its logging behavior. By setting the `OPENLINEAGE_DBT_LOGGING` environment variable, you can establish the logging level for the `openlineage.dbt` and its child modules.

You can also set log level of `dbtol` Python module but this is deprecated.

## Usage

To begin collecting dbt metadata with OpenLineage, replace `dbt run` with `dbt-ol run`.

Additional table and column level metadata will be available if `catalog.json`, a result of running `dbt docs generate`, will be found in the target directory.

If you need to send events without running the job you can use the command `dbt-ol send-events`, it will send the metadata of your last run without running the job.

Commands other than `run`, `build`, `test`, `seed`, `snapshot` and `retry` produce no lineage: `dbt-ol` runs them as plain `dbt`, without sending OpenLineage events. Earlier versions sent a wrapper START and COMPLETE/FAIL event for commands like `compile`, `run-operation` and `docs generate` in the default mode; they no longer do.

### dbt-polars

[dbt-polars](https://github.com/datamindedbe/dbt-polars) runs models in-process with Polars and stores tables
in the catalogs listed in the profile. `openlineage-dbt` supports its Iceberg catalogs that pyiceberg reaches
through a REST or a SQL catalog:

```bash
$ pip3 install openlineage-dbt 'dbt-polars[iceberg]'
$ dbt-ol build
```

For dbt-polars, the `database` of a relation is the name of a catalog in the profile, which is local to the
project. Datasets are named after the physical catalog instead:

| pyiceberg catalog | Namespace                                       | Name                                       |
|:------------------|:------------------------------------------------|:-------------------------------------------|
| REST              | catalog `uri`, e.g. `https://catalog.example.com/api/catalog`, or `{uri}/v1/{prefix}` when the profile sets a `prefix` | `{warehouse}.{schema}.{table}`, or `{schema}.{table}` without a `warehouse` |
| SQL               | catalog `uri`, e.g. `sqlite:////home/me/project/catalog.db` or `postgresql://host:5433/iceberg` | `{catalog name in the profile}.{schema}.{table}` |

The `uri` is written without user name, password, query string, SQLAlchemy driver or default port (HTTP(S),
PostgreSQL, MySQL), with a lowercase scheme and host and IP addresses in their shortest form. A REST `uri`
also loses its fragment and trailing slash. A SQL `uri` is read as SQLAlchemy reads it: `#` is part of the
database, the database is percent-decoded, and for PostgreSQL a `dbname` query parameter overrides it. Its
server may also be given by `host` and `port` query parameters; a `uri` that does not name exactly one network
server, at most one port and a database (several hosts, a Unix socket, conflicting values) is not supported. A REST `warehouse` that is a URL
is cleaned like a REST `uri`, except that the container before `@` in an Azure storage URL (`abfs`, `abfss`,
`wasb`, `wasbs`) is kept. Other catalog options, such as `token` or `credential`, are never read, and a
catalog's options are read only once a dataset uses it. A SQL catalog's own name is part of the dataset name because
pyiceberg keeps the tables of several catalogs in one database and tells them apart by that name. A SQLite
catalog is named after its database file: a relative path is resolved against the dbt project directory, as
dbt-polars opens it, and every path is made absolute with symbolic links resolved, so each spelling of the
same file gets one namespace. A REST `prefix` is part of the namespace because pyiceberg sends every table
request to `{uri}/v1/{prefix}`.

Each model, source and test uses the namespace of its own catalog, so a project that sets `catalog` on
some models or sources, or reads across several catalogs, gets one namespace per catalog.

Limitations:

* Other dbt-polars catalogs (local files, S3, Azure, Databricks, Fabric, Snowflake), other pyiceberg catalog
  types (Glue, Hive, ...), `py-catalog-impl`, in-memory SQL catalogs and catalogs whose `uri` comes from
  `.pyiceberg.yaml` or `PYICEBERG_*` variables instead of the profile are not supported. Listing them in the
  profile is fine, but lineage processing fails with an error when a model, seed, snapshot or source uses one,
  rather than reporting a dataset that could not be identified. A data test writes to its catalog only when
  it stores its failures (`store_failures` or `store_failures_as` in its config, else `--store-failures` or
  `DBT_STORE_FAILURES`); otherwise it has no output dataset. With dbt Core v2 telemetry, a dbt-polars node
  missing from the manifest has no output dataset, since its catalog is unknown.
* A REST catalog is identified by the `uri` and `warehouse` in the profile; vendor-specific dataset names, such
  as `unitycatalog://` for Unity Catalog's Iceberg REST endpoint, are not derived.
* Column-level lineage comes from parsing the compiled SQL and was tested with simple projections only. When
  the parser does not understand a statement, a column comes from a catalog that cannot be identified, or
  the SQL names a table in a way that matches inputs in several catalogs, the `columnLineage` facet is left
  out; table-level lineage is still reported. Python models have no column-level
  lineage.

### dbt Core v2

#### Upgrading from dbt Core v1

* **Node jobs get new names.** On dbt Core v2, model, seed, snapshot, test and unit test jobs are named by
  their dbt `unique_id` (for example `model.jaffle_shop.orders`), as with `--consume-structured-logs` on
  dbt Core v1. The default dbt Core v1 mode names them `{database}.{schema}.{alias}`, with a suffix that
  depends on the command (`analytics.staging.orders` under `run`, `analytics.staging.orders.build.run`
  under `build`). After the upgrade, your lineage backend shows these as new jobs, and their history stays
  under the old names. A `unique_id` stays the same whichever command runs the node.
* **Unit tests have no datasets.** A unit test runs on mocked rows, so its events have no inputs or
  outputs. The `tested_node_unique_id` field of the `dbt_node` job facet names the model it tests. The same
  applies to `--consume-structured-logs` on dbt Core v1.

With dbt Core v2, `dbt-ol` always sends events while the models run. `--consume-structured-logs` isn't
needed and changes nothing. `dbt-ol` points dbt's OpenTelemetry output (`DBT_OTEL_FILE_NAME`) at a
temporary directory and reads it as dbt writes it.

If you set the OTel file yourself (`--otel-file-name` or `DBT_OTEL_FILE_NAME`), `dbt-ol` reads it in place,
starting where it ends when dbt starts. Make sure only one dbt run writes to it at a time: `dbt-ol` takes
the first invocation it finds after that point as its own.

dbt Core v2 writes `manifest.json` only after every node has run, and its telemetry doesn't say which
tables a node reads. So `dbt-ol` runs `dbt parse` before the command, into the same temporary directory,
and takes node inputs from that manifest. The parse doesn't connect to the warehouse and doesn't touch
your `target/` directory.

* `--ol-skip-parse` (or `OPENLINEAGE_DBT_SKIP_PARSE=true`) skips the parse. Node start events then carry
  only the node's own output, and final node events are sent with inputs once dbt writes `manifest.json`
  at the end of the run.
* `OPENLINEAGE_DBT_PARSE_TIMEOUT` sets how many seconds the parse may take, 600 by default. A parse that
  takes longer is stopped, and the run goes ahead as if it had failed.
* `OPENLINEAGE_DBT_TEMP_DIR` sets where the temporary directory is created; by default it's the system
  temporary directory (`TMPDIR`). On large projects the telemetry file is about 23 KB per node, and the
  directory is removed when the run ends.

If node events have no inputs, check the `dbt_run` facet of the wrapper run: `pre_parse` says how the parse
went (`succeeded`, `failed`, `timed_out` or `skipped`), and `write_json` says whether dbt writes
`manifest.json` at the end of the run. Without either manifest, `dbt-ol` can't tell what a node reads.

`dbt-ol` keeps the parsed manifest in memory while dbt runs, which takes about 7 to 8 times the size of
`manifest.json`: about 1.2–1.4 GB for a 10,000-model project, on which dbt itself peaked at about 12 GB.

Any command runs without OpenLineage events when `dbt-ol` can't read the project or profile. On Ctrl-C, dbt stops as usual and `dbt-ol`
keeps sending events until dbt exits, so the runs still open are closed. A second Ctrl-C kills dbt and
stops `dbt-ol` without waiting. SIGTERM is passed on to dbt.

## Development

For development setup instructions, see the [Python development setup guide](https://openlineage.io/docs/client/python/development/setup).

Quick start:
```bash
# From the repository root (requires https://taskfile.dev)
$ task integrations:dbt:setup

# Or manually
$ cd integration/dbt
$ uv sync --extra dev
```

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
