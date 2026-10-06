## OpenLineage-Prefect Quickstart Guide

This guide demos how to spin up a **Marquez** instance for OpenLineage visualization and how to configure Prefect to send OpenLineage events to the Marquez API.

**Required**: an active local Prefect instance. This guide assumes the server and API are using port 4200.

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
    export OPENLINEAGE__TRANSPORT__ENDPOINT="/api/v1/lineage" &&
    export PREFECT_API_URL="http://localhost:4200/api"
    ```

3. **Copy and deploy quickstart.py in your local Prefect instance:**
    ```sh
    prefect deploy -n prefect-test 
    ```

4. **Trigger a run of prefect-test in the Prefect UI.**

5. **Verify event tracking in Marquez:**
    * Watch the flow completion logs pop up in your **Prefect UI**.
    * Switch tabs to the **Marquez UI** to see the structural metadata graphs dynamically drawn from your code run.
