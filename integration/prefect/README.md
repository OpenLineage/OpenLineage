# Openlineage Prefect

The `openlineage-prefect` integration collects, adapts, and emits Prefect flow and task metadata in the OpenLineage specification for data lineage.

## Features

The integration converts Prefect's unique run objects to idempotent tasks and flows, enabling the analysis of both over time in OpenLineage-compatible consumers. 

**Note:** to override this default behavior and preserve unique job names, set the JOB_NAME_TYPE env variable to "full". Setting it to "base" reverts to the default idempotent names.
```sh
export JOB_NAME_TYPE="full"
```

The integration supports OpenLineage Datasets, which enable the aggregation of a data source's producers and consumers across OpenLineage-compatible systems and platforms. This allows for the tracking of external upstream and downstream dependencies of Prefect data sources together with their producing and consuming tasks in Prefect flows. For more details, see "Leveraging Datasets" below.

All transport options offered by `openlineage-python` are available, including intelligent routing of emitted events to Datadog and GCP, async http, http, and composite.

## Basic Configuration

At a minimum, define a namespace, Prefect API URL, and transport consisting of a type, URL, and endpoint. All three can be supplied using environment variables.

For example:

```sh
export OPENLINEAGE_NAMESPACE="prefect_test" &&
export OPENLINEAGE__TRANSPORT__TYPE="http" &&
export OPENLINEAGE__TRANSPORT__URL="http://lineageconsumer.com:5000" &&
export OPENLINEAGE__TRANSPORT__ENDPOINT="/api/v1/lineage" &&
export PREFECT_API_URL="http://prefecthost.com:4200/api"
```

For more details of OpenLineage transport options and how to configure them, consult the [OpenLineage Python Client Documentation](https://openlineage.io/docs/client/python/).

## Leveraging Datasets

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
```

## Quickstart Guide

This guide explains how to spin up a **Marquez** instance for OpenLineage visualization and then configure a Prefect flow to send OpenLineage events to the Marquez API.

**Required**: an active local Prefect instance. This guide assumes the server and API are using port 4200.

1. **Spin up Marquez:**
    ```sh
    mkdir prefect-test
    cd prefect-test
    git clone git@github.com:ilum-cloud/marquez.git
    cd marquez
    ./docker/up.sh --db-port 2345
    ..
    ```
    * **Marquez Lineage UI:** [http://localhost:3000](http://localhost:3000)

2. **Install local environment requirements:**
    ```sh
    python3 -m venv .venv
    source .venv/bin/activate
    pip install prefect openlineage-prefect openlineage-python duckdb
    ```

3. **Configure the integration:**
    ```sh
    export OPENLINEAGE_NAMESPACE="prefect_test" &&
    export OPENLINEAGE__TRANSPORT__TYPE="http" &&
    export OPENLINEAGE__TRANSPORT__URL="http://localhost:5000" &&
    export OPENLINEAGE__TRANSPORT__ENDPOINT="/api/v1/lineage" &&
    export PREFECT_API_URL="http://localhost:4200/api"
    ```

4. **Copy and paste the below code into new file quickstart.py:**

    ```py
    import random
    from prefect import flow, task
    import duckdb
    from prefect.artifacts import create_table_artifact

    ORG_CUSTOMERS_TABLE = [{"database_uri":"duckdb:///company_customers_db", "table":"org_customers"}]
    FIRM_CUSTOMERS_TABLE = [{"database_uri":"duckdb:///company_customers_db", "table":"firm_customers"}]

    @task
    def load_customers():
        """Flaky task"""
        choices = [1, 1, 1, 1, 1, 1, 1, 1, 1, 0]
        choice = random.choice(choices)
        if choice == 1:
            with duckdb.connect("company_customers_db.duckdb") as connector:
                customers = connector.execute("SELECT * FROM org_customers").fetchall()
        else:
            with duckdb.connect("company_customers_db.duckdb") as connector:
                customers = connector.execute("SELECT * FROM org_customer").fetchall() # fails

        create_table_artifact(
            key="get-org-customers",
            table=ORG_CUSTOMERS_TABLE,
            description="ol-dataset_input"
        )

        with duckdb.connect("company_customers_db.duckdb") as connector:
            connector.execute(
                "CREATE TABLE IF NOT EXISTS firm_customers (name VARCHAR, address VARCHAR)"
            )
            connector.executemany(
                "INSERT INTO firm_customers VALUES (?, ?)", customers
            )

        create_table_artifact(
            key="load-firm-customers",
            table=FIRM_CUSTOMERS_TABLE,
            description="ol-dataset_output"
        )

    @task
    def extract_customers():
        with duckdb.connect("company_customers_db.duckdb") as connector:
            connector.execute("DROP TABLE IF EXISTS org_customers")
            connector.execute(
                "CREATE TABLE IF NOT EXISTS org_customers (name VARCHAR, address VARCHAR)"
            )
            connector.execute(
                "INSERT INTO org_customers VALUES (?, ?)", ["Marvin Martian", "1 Mars Pl"]
            )

        create_table_artifact(
            key="org-customers",
            table=ORG_CUSTOMERS_TABLE,
            description="ol-dataset_output"
        )    

    @flow(name="Update Customers")
    def update_customers():
        extract_customers()
        load_customers()

    if __name__ == "__main__":
        update_customers()
    ```

5. **Copy and paste the below code into new file listener.py:**

    ```py
    import asyncio
    from openlineage_prefect.prefect_adapter.listener import PrefectOpenLineageListener

    async def main():
        await PrefectOpenLineageListener().collect_and_process_runs()

    if __name__ == "__main__":
        asyncio.run(main())
    ```

    Run the file in its own process:

    ```py
    python listener.py
    ```

6. **Deploy the Update Customers flow in your local Prefect instance:**

    ```sh
    prefect deploy -n prefect-test 
    ```

7. **Trigger a run of prefect-test in the Prefect UI.**

8. **Verify event tracking in Marquez:**

    * Watch the flow completion logs pop up in your **Prefect UI** at https://localhost:4200.
    * Switch tabs to the **Marquez UI** on https://localhost:3000 to see the lineage metadata graphs dynamically drawn from your code run.
