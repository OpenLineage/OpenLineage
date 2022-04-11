# OpenLineage Dagster Integration

A library that integrates [Dagster](https://dagster.io/) with [OpenLineage](https://openlineage.io) for automatic metadata collection.
It provides an OpenLineage sensor, a Dagster sensor that tails Dagster event logs for tracking metadata.
On each sensor evaluation, the function processes a batch of event logs, converts Dagster events into OpenLineage events, 
and emits them to an OpenLineage backend.

## Features

**Metadata**

* Dagster job & op lifecycle

## Requirements

- [Python 3.7](https://www.python.org/downloads)
- [Dagster 0.13.8+](https://dagster.io/)

## Installation

```bash
$ python -m pip install openlineage-dagster
```

## Usage

### OpenLineage Sensor & Event Log Storage Requirements

**Single OpenLineage sensor per Dagster instance** <br />
As it processes all event logs for a given Dagster instance, define and enable only a single OpenLineage sensor per instance. 
Running multiple will result in emitting duplicate OpenLineage job runs for Dagster steps with different OpenLineage run ids that are dynamically generated during sensor runs.

**Non-sharded [Event Log Storage](https://docs.dagster.io/deployment/dagster-instance#event-log-storage)** <br />
For the sensor to handle all event logs across runs, use non-sharded event log storage.
If an event log storage sharded by run (i.e. default `SqliteEventLogStorage`) is used, cursor that tracks the last processed event storage id may not update properly. 

### OpenLineage Sensor Setup

Get OpenLineage sensor definition from `openlineage_sensor()` factory function and add it to your Dagster repository.

```python
from dagster import repository
from openlineage.dagster.sensor import openlineage_sensor


@repository
def my_repository():
    openlineage_sensor_def = openlineage_sensor()
    return other_defs + [openlineage_sensor_def]
```

With parallel sensor run not supported at the time of writing, some tuning may be necessary to avoid affecting other sensors' performance.

See Dagster's documentation on [Evaluation Interval](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#evaluation-interval)
for more detail on `minimum_interval_seconds`, which defaults to 30 seconds.
`record_filter_limit` is the maximum number of event logs to process on each sensor evaluation, and it defaults to 30 records per evaluation.
Default values can be overridden as below.

```python
@repository
def my_repository():
    openlineage_sensor_def = openlineage_sensor(
        minimum_interval_seconds=60,
        record_filter_limit=60,
    )
    return other_defs + [openlineage_sensor_def]
```


OpenLineage sensor handles event logs in ascending order of storage id, and by default, starts with the first log.
Optionally, `after_storage_id` can be specified to customize the starting point.
This is only applicable when cursor is undefined or has been deleted.

```python
@repository
def my_repository():
    openlineage_sensor_def = openlineage_sensor(
        after_storage_id=100
    )
    return other_defs + [openlineage_sensor_def]
```

### OpenLineage Adapter & Client Configuration

The sensor uses OpenLineage adapter and client to convert and push data to an OpenLineage backend,
and they depend on the following environment variables.

If using User Repository Deployments, add the variables to the repository where the sensor is defined.
Otherwise, add the variables to Dagster Daemon.

* `OPENLINEAGE_URL` - point to service which will consume OpenLineage events
* `OPENLINEAGE_API_KEY` - set if consumer of OpenLineage events requires `Bearer` authentication key
* `OPENLINEAGE_NAMESPACE` - set if you are using something other than the `default` as the default namespace when Dagster repository is undefined 

#### OpenLineage Namespace & Dagster Repository

For Dagster jobs organized in repositories, Dagster keeps track of the repository name for each pipeline run.
When the repository name is present, it is always used as the OpenLineage namespace name.
`OPENLINEAGE_NAMESPACE` option is a way to fall back and provide some other static default value. 


## Development

To install all dependencies for local development:

```bash
$ python -m pip install -e .[dev]  # or python -m pip install -e .\[dev\] in zsh 
```

To run test suite:

```bash
$ pytest
```