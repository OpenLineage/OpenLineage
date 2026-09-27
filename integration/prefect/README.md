# Openlineage Prefect

The `openlineage-prefect` integration collects and adapts Prefect flow and task metadata to the OpenLineage specification for data lineage. All transport options offered by `openlineage-python` are available, including intelligent routing of emitted events to Datadog and GCP, async http, http, and composite.

## Features

The integration converts Prefect's unique run objects to idempotent tasks and flows, enabling the analysis of both over time in OpenLineage-compatible consumers.

Support for OpenLineage Datasets enables the aggregation of a data source's producers and consumers across OpenLineage-compatible systems and platforms. This allows for the tracking of external upstream and downstream dependencies of Prefect data sources together with their producing and consuming tasks in Prefect flows.

## Basic Configuration

At a minimum, define a namespace, Prefect API URL, and transport consisting of a type, URL, and endpoint. All three can be supplied using environment variables.

For example:

```sh
export OPENLINEAGE_NAMESPACE='prefect_test' &&
export OPENLINEAGE__TRANSPORT__TYPE='http' &&
export OPENLINEAGE__TRANSPORT__URL='http://lineageconsumer.com:5000' &&
export OPENLINEAGE__TRANSPORT__ENDPOINT='/api/v1/lineage' &&
export PREFECT_API_URL='http://prefecthost.com:4200/api'
```

For more details of OpenLineage transport options and how to configure them, consult the [OpenLineage Python Client Documentation](https://openlineage.io/docs/client/python/).

## Datasets

The integration looks for OpenLineage Datasets in Prefect Artifacts. To attach an input or output Dataset to a job run, use `create_table_artifact` from the Prefect Artifact library to create an Artifact in a task definition context. Provide a namespace -- e.g., the dataset URI -- and name via an Artifact's `table` and `description`, respectively. Distinguish the type of Dataset by appending `_output` or `_input` to the description.

For example:

```py
ol_table = [{"database_uri":"duckdb:///customers_db", "table":"customers"}]

create_table_artifact(
    key="upstream-insert",
    table=ol_table,
    description="ol-dataset_output"
)
```

## Execution

Import the `PrefectOpenLineageListener` class and execute the `collect_and_process_runs` method asynchronously in its own process.

For example:

```py
import asyncio
from openlineage_prefect.prefect_adapter.listener import PrefectOpenLineageListener

async def main():
    await PrefectOpenLineageListener().collect_and_process_runs()

if __name__ == "__main__":
    asyncio.run(main())
```
