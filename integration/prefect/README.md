# OpenLineage Prefect Integration

A library that integrates [Prefect](https://www.prefect.io/) `Flows` with [OpenLineage](https://openlineage.io) for automatic metadata collection.

## Features

**Metadata**

* Flow and task metadata
* Flow parameters
* Task runs linked to **versioned** code
* Task inputs / outputs

**Lineage**

* Track Flow dependencies

## Requirements

- [Python >= 3.7](https://www.python.org/downloads)
- [Prefect >= 0.15.0](https://docs.prefect.io/) (may work for earlier versions)

## Installation

```bash
$ pip3 install openlineage-prefect
```

## Configuration


### `HTTP` Backend Environment Variables

`openlineage-prefect` uses OpenLineage client to push data to OpenLineage backend.

OpenLineage client depends on environment variables:

* `OPENLINEAGE_URL` - point to service which will consume OpenLineage events
* `OPENLINEAGE_API_KEY` - set if consumer of OpenLineage events requires `Bearer` authentication key
* `OPENLINEAGE_NAMESPACE` - set if you are using something other than the `default` namespace for job namespace.

For backwards compatibility, `openlineage-prefect` also support configuration via
`MARQUEZ_URL`, `MARQUEZ_NAMESPACE` and `MARQUEZ_API_KEY` variables.

```
MARQUEZ_URL=http://my_hosted_marquez.example.com:5000
MARQUEZ_NAMESPACE=my_special_ns
```

## Usage

To begin collecting Prefect Flow metadata with OpenLineage, use:

```diff
from openlineage.prefect import OpenLineageFlowRunner

- flow.run()
+ flow.run(runner_cls=OpenLineageFlowRunner)
```

When enabled, the library will:

1. On DAG **start**, collect metadata for each task using an `Extractor` (the library defines a _default_ extractor to use otherwise)
2. Collect task input / output metadata (`source`, `schema`, etc)
3. Collect task run-level metadata (execution time, state, parameters, etc)
4. On DAG **complete**, also mark the task as _complete_ in OpenLineage

## Development

To install all dependencies for _local_ development:

```bash
# Bash
$ pip3 install -e .[dev]
```
```zsh
# escape the brackets in zsh
$ pip3 install -e .\[dev\]
```

To run the test suite:

```bash
$ pytest
```
