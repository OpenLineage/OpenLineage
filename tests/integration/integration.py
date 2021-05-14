# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging

import psycopg2

from airflow.utils.state import State as DagState
from airflow.version import version as AIRFLOW_VERSION

from marquez_airflow import __version__ as MARQUEZ_AIRFLOW_VERSION
from marquez_client import MarquezClient
from marquez_client.models import (
    DatasetType,
    JobType,
    RunState
)

from retrying import retry

log = logging.getLogger(__name__)

NAMESPACE_NAME = 'food_delivery'

# AIRFLOW
DAG_ID = 'orders_popular_day_of_week'
DAG_OWNER = 'anonymous'
DAG_DESCRIPTION = \
    'Determines the popular day of week orders are placed.'
IF_NOT_EXISTS_TASK_ID = 'if_not_exists'
INSERT_TASK_ID = 'insert'

# SOURCE
SOURCE_NAME = 'food_delivery_db'
CONNECTION_URL = \
    'postgres://food_delivery:food_delivery@postgres:5432/food_delivery'

# DATASETS
IN_TABLE_NAME = 'public.top_delivery_times'
IN_TABLE_PHYSICAL_NAME = IN_TABLE_NAME
OUT_TABLE_NAME = 'public.popular_orders_day_of_week'
OUT_TABLE_PHYSICAL_NAME = OUT_TABLE_NAME
OUT_TABLE_FIELDS = [
    {
        'name': 'order_day_of_week',
        'type': 'VARCHAR',
        'tags': [],
        'description': None
    },
    {
        'name': 'order_placed_on',
        'type': 'TIMESTAMP',
        'tags': [],
        'description': None
    },
    {
        'name': 'orders_placed',
        'type': 'INT4',
        'tags': [],
        'description': None
    }
]

client = MarquezClient(url='http://marquez:5000')

airflow_db_conn = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="airflow",
    password="airflow"
)
airflow_db_conn.autocommit = True


@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000
)
def wait_for_dag():
    log.info(
        f"Waiting for DAG '{DAG_ID}'..."
    )

    cur = airflow_db_conn.cursor()
    cur.execute(
        f"""
        SELECT dag_id, state
          FROM dag_run
         WHERE dag_id = '{DAG_ID}';
        """
    )
    row = cur.fetchone()
    dag_id = row[0]
    dag_state = row[1]

    cur.close()

    log.info(f"DAG '{dag_id}' state set to '{dag_state}'.")
    if dag_state != DagState.SUCCESS:
        raise Exception('Retry!')


def check_namespace_meta():
    namespace = client.get_namespace(NAMESPACE_NAME)
    assert namespace['name'] == NAMESPACE_NAME
    assert namespace['ownerName'] == DAG_OWNER
    assert namespace['description'] is None


def check_source_meta():
    source = client.get_source(SOURCE_NAME)
    assert source['type'] == 'POSTGRESQL'
    assert source['name'] == SOURCE_NAME
    assert source['connectionUrl'] == CONNECTION_URL
    assert source['description'] is None


def check_datasets_meta():
    in_table = client.get_dataset(
        namespace_name=NAMESPACE_NAME,
        dataset_name=IN_TABLE_NAME
    )
    assert in_table['id'] == {
        'namespace': NAMESPACE_NAME,
        'name': IN_TABLE_NAME
    }
    assert in_table['type'] == DatasetType.DB_TABLE.value
    assert in_table['name'] == IN_TABLE_NAME
    assert in_table['physicalName'] == IN_TABLE_PHYSICAL_NAME
    assert in_table['namespace'] == NAMESPACE_NAME
    assert in_table['sourceName'] == SOURCE_NAME
    assert len(in_table['fields']) == 0
    assert len(in_table['tags']) == 0
    assert in_table['lastModifiedAt'] is None
    assert in_table['description'] is None

    out_table = client.get_dataset(
        namespace_name=NAMESPACE_NAME,
        dataset_name=OUT_TABLE_NAME
    )

    assert out_table['id'] == {
        'namespace': NAMESPACE_NAME,
        'name': OUT_TABLE_NAME
    }
    assert out_table['type'] == DatasetType.DB_TABLE.value
    assert out_table['name'] == OUT_TABLE_NAME
    assert out_table['physicalName'] == OUT_TABLE_PHYSICAL_NAME
    assert out_table['namespace'] == NAMESPACE_NAME
    assert out_table['sourceName'] == SOURCE_NAME
    assert out_table['fields'] == OUT_TABLE_FIELDS
    assert len(out_table['tags']) == 0
    # TODO: marquez does not update this on openlineage event
    # assert out_table['lastModifiedAt'] is not None
    assert out_table['description'] is None


def check_jobs_meta():
    if_not_exists_job = client.get_job(
        namespace_name=NAMESPACE_NAME,
        job_name=f"{DAG_ID}.{IF_NOT_EXISTS_TASK_ID}"
    )

    assert if_not_exists_job['id'] == {
        'namespace': NAMESPACE_NAME,
        'name': f"{DAG_ID}.{IF_NOT_EXISTS_TASK_ID}"
    }
    assert if_not_exists_job['type'] == JobType.BATCH.value
    assert if_not_exists_job['namespace'] == NAMESPACE_NAME
    assert len(if_not_exists_job['inputs']) == 0
    assert len(if_not_exists_job['outputs']) == 0
    assert if_not_exists_job['location'] is None

    # TODO: waiting for backend fix
    # assert if_not_exists_job['context']['sql'] is not None
    # assert if_not_exists_job['description'] == DAG_DESCRIPTION
    # TODO: no airflow context data yet
    # assert if_not_exists_job['context']['airflow.operator'] == \
    #        'airflow.operators.postgres_operator.PostgresOperator'
    # assert if_not_exists_job['context']['airflow.task_info'] is not None
    # assert if_not_exists_job['context']['airflow.version'] == AIRFLOW_VERSION
    # assert if_not_exists_job['context']['marquez_airflow.version'] == MARQUEZ_AIRFLOW_VERSION
    assert if_not_exists_job['latestRun']['state'] == RunState.COMPLETED.value

    insert_job = client.get_job(
        namespace_name=NAMESPACE_NAME,
        job_name=f"{DAG_ID}.{INSERT_TASK_ID}"
    )

    assert insert_job['id'] == {
        'namespace': NAMESPACE_NAME,
        'name': f"{DAG_ID}.{INSERT_TASK_ID}"
    }
    assert insert_job['type'] == JobType.BATCH.value
    assert insert_job['namespace'] == NAMESPACE_NAME
    assert insert_job['inputs'] == [{
        'namespace': NAMESPACE_NAME,
        'name': IN_TABLE_NAME
    }]
    assert insert_job['outputs'] == [{
        'namespace': NAMESPACE_NAME,
        'name': OUT_TABLE_NAME
    }]
    assert insert_job['location'] is None
    # TODO: waiting for backend
    # assert insert_job['context']['sql'] is not None
    # assert insert_job['description'] == DAG_DESCRIPTION
    # TODO: no airflow context job data yet
    # assert insert_job['context']['airflow.operator'] == \
    #        'airflow.operators.postgres_operator.PostgresOperator'
    # assert insert_job['context']['airflow.task_info'] is not None
    # assert insert_job['context']['airflow.version'] == AIRFLOW_VERSION
    # assert insert_job['context']['marquez_airflow.version'] == MARQUEZ_AIRFLOW_VERSION
    assert insert_job['latestRun']['state'] == RunState.COMPLETED.value


def check_jobs_run_meta():
    if_not_exists_job = client.get_job(
        namespace_name=NAMESPACE_NAME,
        job_name=f"{DAG_ID}.{IF_NOT_EXISTS_TASK_ID}"
    )
    if_not_exists_job_run = client.get_job_run(
        run_id=if_not_exists_job['latestRun']['id']
    )
    assert if_not_exists_job_run['id'] == if_not_exists_job['latestRun']['id']
    assert if_not_exists_job_run['state'] == RunState.COMPLETED.value

    insert_job = client.get_job(
        namespace_name=NAMESPACE_NAME,
        job_name=f"{DAG_ID}.{INSERT_TASK_ID}"
    )
    insert_job_run = client.get_job_run(
        run_id=insert_job['latestRun']['id']
    )
    assert insert_job_run['id'] == insert_job['latestRun']['id']
    assert insert_job_run['state'] == RunState.COMPLETED.value


def main():
    # (1) Wait for DAG to complete
    wait_for_dag()
    # (2) Run checks on DAG metadata collected
    check_namespace_meta()
    check_source_meta()
    check_datasets_meta()
    check_jobs_meta()
    check_jobs_run_meta()


if __name__ == "__main__":
    main()
