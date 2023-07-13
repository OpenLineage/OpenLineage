# OpenLineage dbt integration

Wrapper script for automatic metadata collection from dbt

## Features

**Metadata**

* Model run lifecycle
* Model inputs / outputs

## Requirements

- [Python >= 3.8](https://www.python.org/downloads)
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

`openlineage-dbt` uses the OpenLineage client to push data to the OpenLineage backend.

The OpenLineage client depends on environment variables:

* `OPENLINEAGE_URL` - point to service which will consume OpenLineage events
* `OPENLINEAGE_API_KEY` - set if consumer of OpenLineage events requires `Bearer` authentication key
* `OPENLINEAGE_NAMESPACE` - set if you are using something other than the `default` namespace for job namespace.


### Logging

In addition to conventional logging approaches, the OpenLineage dbt wrapper script provides an alternative way of configuring its logging behavior. By setting the `OPENLINEAGE_DBT_LOGGING` environment variable, you can establish the logging level for the `openlineage.dbt` and its child modules.

You can also set log level of `dbtol` which is deprecated.

## Usage

To begin collecting dbt metadata with OpenLineage, replace `dbt run` with `dbt-ol run`.

Additional table and column level metadata will be available if `catalog.json`, a result of running `dbt docs generate`, will be found in the target directory.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project