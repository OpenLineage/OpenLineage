---
sidebar_position: 1
title: Prefect
---

## Overview

The `openlineage-prefect` integration collects, adapts, and emits information from Prefect using the OpenLineage Python Client.

[**Prefect**](https://github.com/PrefectHQ/prefect) is an orchestration tool for building data pipelines with Python.

## Features

The Prefect integration leverages the `prefect-client` library to subscribe to events logged by tasks and flows executed locally by a Prefect worker. The user can opt to convert the unique task and flow objects logged by the worker to idempotent objects (the default configuration) or collect them without conversion. Code modification is not required for a basic implementation.

Standard OpenLineage facets employed include the `JobTypeJobFacet`, `NominalTimeRunFacet`, `ProcessingEngineRunFacet`, and `ParentRunFacet` (for attaching flow run information to task run events). A new custom facet, `PrefectDeploymentRunFacet`, adds deployment information to flow run and task run events.

## Configuration

Configuration required of the user is minimal.

**Required:** Define a namespace, Prefect API URL, and transport consisting of a type, URL, and endpoint. All three can be supplied using environment variables.

For example:

```sh
export OPENLINEAGE_NAMESPACE="prefect_test" &&
export OPENLINEAGE__TRANSPORT__TYPE="http" &&
export OPENLINEAGE__TRANSPORT__URL="http://lineageconsumer.com:5000" &&
export OPENLINEAGE__TRANSPORT__ENDPOINT="/api/v1/lineage" &&
export PREFECT_API_URL="http://prefecthost.com:4200/api"
```

For more details of OpenLineage transport options and how to configure them, consult the [OpenLineage Python Client Documentation](https://openlineage.io/docs/client/python/).

### Including Datasets in Events

The integration looks for OpenLineage Datasets in Prefect Artifacts. To attach an input or output Dataset to a job run, use `create_table_artifact` from the Prefect Artifact library to create an Artifact in a task definition context. Provide a database URI and table name via an Artifact's `table` and `description`, respectively, as shown below. Distinguish the type of Dataset by appending `_output` or `_input` to `ol-dataset` in the description.

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

