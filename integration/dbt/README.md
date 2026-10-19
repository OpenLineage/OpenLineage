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

## Installation

```bash
$ pip3 install openlineage-dbt
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

### Logging

In addition to conventional logging approaches, the OpenLineage dbt wrapper script provides an alternative way of configuring its logging behavior. By setting the `OPENLINEAGE_DBT_LOGGING` environment variable, you can establish the logging level for the `openlineage.dbt` and its child modules.

You can also set log level of `dbtol` Python module but this is deprecated.

## Usage

To begin collecting dbt metadata with OpenLineage, replace `dbt run` with `dbt-ol run`.

Additional table and column level metadata will be available if `catalog.json`, a result of running `dbt docs generate`, will be found in the target directory.

If you need to send events without running the job you can use the command `dbt-ol send-events`, it will send the metadata of your last run without running the job.

### dbt Core v2

With dbt Core v2, `dbt-ol` always sends events while the models run. `--consume-structured-logs` isn't
needed and changes nothing. `dbt-ol` points dbt's OpenTelemetry output (`DBT_OTEL_FILE_NAME`) at a
temporary directory and reads it as dbt writes it.

dbt Core v2 writes `manifest.json` only after every node has run, and its telemetry doesn't say which
tables a node reads. So `dbt-ol` runs `dbt parse` before the command, into the same temporary directory,
and takes node inputs from that manifest. The parse doesn't connect to the warehouse and doesn't touch
your `target/` directory.

* `--ol-skip-parse` (or `OPENLINEAGE_DBT_SKIP_PARSE=true`) skips the parse. Node start events then carry
  only the node's own output, and final node events are sent with inputs once dbt writes `manifest.json`
  at the end of the run.
* `OPENLINEAGE_DBT_TEMP_DIR` sets where the temporary directory is created; by default it's the system
  temporary directory (`TMPDIR`). On large projects the telemetry file is about 23 KB per node, and the
  directory is removed when the run ends.

Commands that produce no lineage, like `dbt deps` or `dbt debug`, run without OpenLineage events, and so
does any command when `dbt-ol` can't read the project or profile. On Ctrl-C, dbt stops as usual and `dbt-ol`
keeps sending events until dbt exits, so the runs still open are closed. A second Ctrl-C kills dbt and
stops `dbt-ol` without waiting. SIGTERM is passed on to dbt.

On dbt Core v2, node jobs are named by their `unique_id` (for example `model.jaffle_shop.orders`), as with
`--consume-structured-logs` on dbt Core v1. Jobs from the default dbt Core v1 mode are named
`{database}.{schema}.{model}`, so their history doesn't continue under the new names.

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
