---
sidebar_position: 2
title: Quickstart
---

## OpenLineage Prefect Guide

This guide demos how to spin up a **Marquez** instance for OpenLineage visualization and how to configure Prefect to send OpenLineage events to the Marquez API.

**Required**: a Prefect instance running locally. This guide assumes the server and API are using port 4200.

1. **Spin up Marquez:**
    ```sh
    git clone git@github.com:ilum-cloud/marquez.git
    cd marquez
    ./docker/up.sh --db-port 2345
    ..
    ```
    * **Marquez Lineage UI:** [http://localhost:3000](http://localhost:3000)

2. **Install local environment requirements:**
    ```sh
    pip install prefect openlineage-prefect openlineage-python duckdb
    ```

3. **Configure the integration:**
    ```sh
    export OPENLINEAGE_NAMESPACE="prefect_test" &&
    export OPENLINEAGE__TRANSPORT__TYPE="http" &&
    export OPENLINEAGE__TRANSPORT__URL="http://localhost:5000" &&
    export OPENLINEAGE__TRANSPORT__ENDPOINT="/api/v1/lineage"
    ```

4. **Copy and deploy the code below to your Prefect instance:**
        
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

6. **Trigger a run of prefect-test in the Prefect UI.**

7. **Verify event tracking in Marquez:**
    * Watch the flow completion logs pop up in your **Prefect UI**.
    * Switch tabs to the **Marquez UI** to see the structural metadata graphs dynamically drawn from your code run.
