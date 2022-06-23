# OpenLineage dbt integration

Wrapper script for dbt for automatic metadata collection

## Features

**Metadata**

* Model run lifecycle
* Model inputs / outputs

## Requirements

- [Python >= 3.7](https://www.python.org/downloads)
- [dbt >= 0.20](https://www.getdbt.com/)

Right now, `openlineage-dbt` only supports `bigquery`, `snowflake`, `spark` and `redshift` dbt adapters.

## Installation

```bash
$ pip3 install openlineage-dbt
```

To install from source, run:

```bash
$ pip install .
```

## Configuration


### `HTTP` Backend Environment Variables

`openlineage-dbt` uses OpenLineage client to push data to OpenLineage backend.

OpenLineage client depends on environment variables:

* `OPENLINEAGE_URL` - point to service which will consume OpenLineage events
* `OPENLINEAGE_API_KEY` - set if consumer of OpenLineage events requires `Bearer` authentication key
* `OPENLINEAGE_NAMESPACE` - set if you are using something other than the `default` namespace for job namespace.


## Usage

To begin collecting dbt metadata with OpenLineage, replace `dbt run` with `dbt-ol run`.

Additional table and column level metadata will be available if `catalog.json`, result of running `dbt docs generate` will be found in target directory.
