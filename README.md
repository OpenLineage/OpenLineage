# marquez-airflow

A library that integrates [Airflow `DAGs`]() with [Marquez](https://github.com/MarquezProject/marquez) for automatic metadata collection.

## Features

**Metadata**

* Task lifecycle
* Task parameters
* Task runs linked to **versioned** code
* Task inputs / outputs

**Lineage**

* Track inter-DAG dependencies

**Built-in**

* SQL parser
* Link to code builder (ex: **GitHub**)
* Metadata extractors

## Requirements

 - [Python 3.6.0](https://www.python.org/downloads)+
 - [Airflow 1.10.4](https://pypi.org/project/apache-airflow)+

## Installation

```bash
$ pip3 install marquez-airflow
```

> **Note:** You can also add `marquez-airflow` to your `requirements.txt` for Airflow.

To install from source, run:

```bash
$ python3 setup.py install
```

## Configuration

The library depends on a _backend_. A `Backend` is configurable and lets the library know where to write dataset, job, and run metadata.

### Backends

* `HTTP`: Write metadata to Marquez
* `FILE`: Write metadata to a file (as `json`) under `/tmp/marquez`
* `LOG`: Simply just logs the metadata to the console

By default, the `HTTP` backend will be used (see next sections on configuration). To override the default backend and write metadata to a file, use `MARQUEZ_BACKEND`:

```
MARQUEZ_BACKEND=FILE
```

> **Note:** Metadata will be written to `/tmp/marquez/client.requests.log`, but the location can be overridden with `MARQUEZ_FILE`.

### `HTTP` Backend Authentication

The `HTTP` backend supports using API keys to authenticate requests via `Bearer` auth. To include a key when making an API request, use `MARQUEZ_API_KEY`:

```
MARQUEZ_BACKEND=HTTP
MARQUEZ_API_KEY=[YOUR_API_KEY]
```

### `HTTP` Backend Environment Variables

`marquez-airflow` needs to know where to talk to the Marquez server API.  You can set these using environment variables to be read by your Airflow service.

You will also need to set the namespace if you are using something other than the `default` namespace.

```
MARQUEZ_BACKEND=HTTP
MARQUEZ_URL=http://my_hosted_marquez.example.com:5000
MARQUEZ_NAMESPACE=my_special_ns
```

### Extractors : Sending the correct data from your DAGs

If you do nothing, Marquez will receive the `Job` and the `Run` from your DAGs, but sources and datasets will not be sent.

`marquez-airflow` allows you to do more than that by building "Extractors".  Extractors are in the process of changing right now, but they basically take a task and extract:

1. Name : The name of the task
2. Location : Location of the code for the task
3. Inputs : List of input datasets
4. Outputs : List of output datasets
5. Context : The Airflow context for the task

It's important to understand the inputs and outputs are lists and relate directly to the `Dataset` object in Marquez.  Datasets also include a source which relates directly to the `Source` object in Marquez.

## Usage

To begin collecting Airflow DAG metadata with Marquez, use:

```diff
- from airflow import DAG
+ from marquez_airflow import DAG
```

When enabled, the library will:

1. On DAG **start**, collect metadata for each task using an `Extractor` (the library defines a _default_ extractor to use otherwise)
2. Collect task input / output metadata (`source`, `schema`, etc)
3. Collect task run-level metadata (execution time, state, parameters, etc)
4. On DAG **complete**, also mark the task as _complete_ in Marquez

To enable logging, set the environment variable `MARQUEZ_LOG_LEVEL` to `DEBUG`, `INFO`, or `ERROR`:

```
$ export MARQUEZ_LOG_LEVEL=INFO
```

## Development

To install all dependencies for _local_ development:

```bash
$ pip3 install -e .[dev]
```

To run the entire test suite, you'll first want to initialize the Airflow database:

```bash
$ airflow initdb
```

Then, run the test suite with:

```bash
$ pytest
```
