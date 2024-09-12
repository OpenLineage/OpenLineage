---
sidebar_position: 2
---
# Getting Started with Airflow and OpenLineage+Marquez

In this example, we'll walk you through how to enable Airflow DAGs to send lineage metadata to [Marquez](https://marquezproject.ai/) using OpenLineage. 

### You’ll Learn How To:

* configure Airflow to send OpenLineage events to Marquez
* write OpenLineage-enabled DAGs
* troubleshoot a failing DAG using Marquez

### Table of Contents

1. [Step 1: Configure Your Astro Project](#configure-your-astro-project)
2. [Step 2: Add Marquez Services Using Docker Compose](#add-marquez-services-using-docker-compose)
3. [Step 3: Start Airflow with Marquez](#start-airflow-with-marquez)
4. [Step 4: Write Airflow DAGs](#write-airflow-dags)
5. [Step 5: View Collected Metadata](#view-collected-metadata)
6. [Step 6: Troubleshoot a Failing DAG with Marquez](#troubleshoot-a-failing-dag-with-marquez)

## Prerequisites

Before you begin, make sure you have installed:

* [Docker 17.05](https://docs.docker.com/install)+
* [Astro CLI](https://docs.astronomer.io/astro/cli/overview)
* [curl](https://curl.se/)

> **Note:** We recommend that you have allocated at least **2 CPUs** and **8 GB** of memory to Docker.

## Configure Your Astro Project

Use the Astro CLI to create and run an Airflow project locally that will integrate with Marquez.

1. In your project directory, create a new Astro project:

    ```sh
    $ ..
    $ mkdir astro-marquez-tutorial && cd astro-marquez-tutorial
    $ astro dev init
    ```

2. Using curl, change into new directory `docker` and download some scripts required by Marquez services:

    ```sh
    $ mkdir docker && cd docker
    $ curl -O "https://raw.githubusercontent.com/MarquezProject/marquez/main/docker/{entrypoint.sh,wait-for-it.sh}"
    $ ..
    ```

    After executing the above, your project directory should look like this:

    ```sh
    $ ls -a
    .                     Dockerfile            packages.txt
    ..                    README.md             plugins
    .astro                airflow_settings.yaml requirements.txt
    .dockerignore         dags                  tests
    .env                  docker
    .gitignore            include
    ```

3. Add the OpenLineage Airflow Provider and the Common SQL Provider to the requirements.txt file:

    ```txt
    apache-airflow-providers-common-sql==1.7.2
    apache-airflow-providers-openlineage==1.1.0
    ```

    For details about the Provider and its minimum requirements, see the Airflow [docs](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html).

4. To configure Astro to send lineage metadata to Marquez, add the following environment variables below to your Astro project's `.env` file:

    ```env
    OPENLINEAGE_URL=http://host.docker.internal:5000
    OPENLINEAGE_NAMESPACE=example
    AIRFLOW_CONN_EXAMPLE_DB=postgres://example:example@host.docker.internal:7654/example
    ```

    These variables allow Airflow to connect with the OpenLineage API and send events to Marquez.

5. It is a good idea to have Airflow use a different port for Postgres than the default 5432, so run the following command to use port 5678 instead:

    ```sh
    astro config set postgres.port 5678
    ```

6. Check the Dockerfile to verify that your installed version of the Astro Runtime is 9.0.0+ (to ensure that you will be using Airflow 2.7.0+).

    For example:

    ```txt
    FROM quay.io/astronomer/astro-runtime:9.1.0
    ```

## Add Marquez and Database Services Using Docker Compose

Astro supports manual configuration of services via Docker Compose using YAML.

Create new file `docker-compose.override.yml` in your project and copy/paste the following into the file:

```yml
version: "3.1"
services:
  web:
    image: marquezproject/marquez-web:latest
    container_name: marquez-web
    environment:
      - MARQUEZ_HOST=api
      - MARQUEZ_PORT=5000
    ports:
      - "3000:3000"
    depends_on:
      - api

  db:
    image: postgres:14.9
    container_name: marquez-db
    ports:
      - "6543:6543"
    environment:
      - POSTGRES_USER=marquez
      - POSTGRES_PASSWORD=marquez
      - POSTGRES_DB=marquez

  example-db:
    image: postgres:14.9
    container_name: example-db
    ports:
      - "7654:5432"
    environment:
      - POSTGRES_USER=example
      - POSTGRES_PASSWORD=example
      - POSTGRES_DB=example
  
  api:
    image: marquezproject/marquez:latest
    container_name: marquez-api
    environment:
      - MARQUEZ_PORT=5000
      - MARQUEZ_ADMIN_PORT=5001
    ports:
      - "5000:5000"
      - "5001:5001"
    volumes:
       - ./docker/wait-for-it.sh:/usr/src/app/wait-for-it.sh
    links:
      - "db:postgres"
    depends_on:
      - db
    entrypoint: ["/bin/bash", "./wait-for-it.sh", "db:6543", "--", "./entrypoint.sh"]

  redis:
    image: bitnami/redis:6.0.6
    environment:
      - ALLOW_EMPTY_PASSWORD=yes
```

The above adds the Marquez API, database and Web UI, along with an additional Postgres database for the DAGs used in this example, to Astro's Docker container and configures them to use the scripts in the `docker` directory you previously downloaded from Marquez.

## Start Airflow with Marquez

Now you can start all services. To do so, execute the following:

```bash
$ astro dev start
```

**The above command will:**

* start Airflow
* start Marquez, including its API, database and UI
* create and start a Postgres server for DAG tasks

To view the Airflow UI and verify it's running, open [http://localhost:8080](http://localhost:8080). Then, log in using the username and password `admin` / `admin`. You can also browse to [http://localhost:3000](http://localhost:3000) to view the Marquez UI.

## Write Airflow DAGs

In this step, you will create two new Airflow DAGs that perform simple tasks. The `counter` DAG adds 1 to a column every minute, while the `sum` DAG calculates a sum every five minutes. This will result in a simple pipeline containing two jobs and two datasets.

### Create a `counter` DAG

In `dags/`, create a file named `counter.py` and add the following code:

```python
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    'counter',
    start_date=days_ago(1),
    schedule='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    description='DAG that generates a new count value equal to 1.'
):

    query1 = PostgresOperator(
        task_id='if_not_exists',
        postgres_conn_id='example_db',
        sql='''
        CREATE TABLE IF NOT EXISTS counts (
            value INTEGER
        );'''
    )

    query2 = PostgresOperator(
        task_id='inc',
        postgres_conn_id='example_db',
        sql='''
        INSERT INTO counts (value)
            VALUES (1) 
        '''
    )

query1 >> query2
```

### Create a `sum` DAG

In `dags/`, create a file named `sum.py` and add the following code:

```python
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    'sum',
    start_date=days_ago(1),
    schedule='*/5 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    description='DAG that sums the total of generated count values.'
):

    query1 = PostgresOperator(
        task_id='if_not_exists',
        postgres_conn_id='example_db',
        sql='''
        CREATE TABLE IF NOT EXISTS sums (
            value INTEGER
        );'''
    )

    query2 = PostgresOperator(
        task_id='total',
        postgres_conn_id='example_db',
        sql='''
        INSERT INTO sums (value)
            SELECT SUM(value) FROM counts;
        '''
    )

query1 >> query2
```

## View Collected Metadata

To ensure that Airflow is executing `counter` and `sum`, navigate to the DAGs tab in Airflow and verify that they are both enabled and are in a _running_ state:

![](./docs/astro-view-dags.png)

To view DAG metadata collected by Marquez from Airflow, browse to the Marquez UI by visiting [http://localhost:3000](http://localhost:3000). Then, use the _search_ bar in the upper right-side of the page and search for the `counter.inc` job. To view lineage metadata for `counter.inc`, click on the job from the drop-down list:

> **Note:** If the `counter.inc` job is not in the drop-down list, check to see if Airflow has successfully executed the DAG.

<p align="center">
  <img src={require("./docs/current-search-count.png").default} />
</p>

If you take a quick look at the lineage graph for `counter.inc`, you should see `example.public.counts` as an output dataset and `sum.total` as a downstream job!

![](./docs/astro-current-lineage-view-job.png)

## Troubleshoot a Failing DAG with Marquez

In this step, let's quickly walk through a simple troubleshooting scenario where the DAG `sum` begins to fail as the result of an upstream schema change for table `counts`.

> **Tip:** It's helpful to apply the same code changes outlined below to your Airflow DAGs defined in **Step 6**.

Let's say team `A` owns the DAG `counter`. Team `A` decides to update the tasks in `counter` to rename the `values` column in the `counts` table to `value_1_to_10` (without properly communicating the schema change!):

```diff
query1 = PostgresOperator(
-   task_id='if_not_exists',
+   task_id='alter_name_of_column',
    postgres_conn_id='example_db',
    sql='''
-   CREATE TABLE IF NOT EXISTS counts (
-     value INTEGER
-   );''',
+   ALTER TABLE "counts" RENAME COLUMN "value" TO "value_1_to_10";
+   '''
)
```

```diff
query2 = PostgresOperator(
    task_id='inc',
    postgres_conn_id='example_db',
    sql='''
-    INSERT INTO counts (value)
+    INSERT INTO counts (value_1_to_10)
         VALUES (1)
    ''',
)
```

Team `B`, unaware of the schema change, owns the DAG `sum` and begins to see DAG run metadata with _failed_ run states:

![](./docs/astro-job-failure.png)

But, team `B` is not sure what might have caused the DAG failure as no recent code changes have been made to the DAG. So, team `B` decides to check the schema of the input dataset:

![](./docs/astro-lineage-view-dataset.png)

Team `B` soon realizes that the schema has changed recently for the `counts` table! To fix the DAG, team `B` updates the `t2` task that calculates the count total to use the new column name:

```diff
query2 = PostgresOperator(
    task_id='total',
    postgres_conn_id='example_db',
    sql='''
    INSERT INTO sums (value)
-       SELECT SUM(value) FROM counts;
+       SELECT SUM(value_1_to_10) FROM counts;
    '''
)
```

With the code change, the DAG `sum` begins to run successfully:

![](./docs/astro-lineage-view-job-successful.png)

_Congrats_! You successfully step through a troubleshooting scenario of a failing DAG using metadata collected with Marquez! You can now add your own DAGs to `dags/` to build more complex data lineage graphs.

## Next Steps

* Review the Marquez [HTTP API](https://marquezproject.github.io/marquez/openapi.html) used to collect Airflow DAG metadata and learn how to build your own integrations using OpenLineage.
* Take a look at [`openlineage-spark`](https://openlineage.io/docs/integrations/spark/) integration that can be used with Airflow.

## Feedback

What did you think of this example? You can reach out to us on [Slack](http://bit.ly/MqzSlack) and leave us feedback, or [open a pull request](https://github.com/MarquezProject/marquez/blob/main/CONTRIBUTING.md#submitting-a-pull-request) with your suggested changes!