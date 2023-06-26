# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import sys
import time
import unittest

import psycopg2
import pytest
import requests
from requests.auth import HTTPBasicAuth
from retrying import retry

logging.basicConfig(
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    level='DEBUG'
)
log = logging.getLogger(__name__)

airflow_db_conn = None


@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000
)
def wait_for_dag(dag_id):
    log.info(
        f"Waiting for DAG '{dag_id}'..."
    )

    cur = airflow_db_conn.cursor()
    cur.execute(
        f"""
        SELECT dag_id, state
            FROM dag_run
        WHERE dag_id = '{dag_id}'
        ORDER BY execution_date DESC
        LIMIT 1;
        """
    )
    row = cur.fetchone()
    dag_id, state = row

    cur.close()

    log.info(f"DAG '{dag_id}' state set to '{state}'.")
    if state == 'failed':
        return False
    elif state != "success":
        raise Exception('Retry!')
    return True


@pytest.fixture(scope="module", autouse=True)
def airflow_db_conn():
    yield setup_db()


def setup_db():
    time.sleep(10)
    global airflow_db_conn
    airflow_db_conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    airflow_db_conn.autocommit = True


@pytest.mark.parametrize("dag_id", ["wait_dag", "test_dag", "hanging_extractor_dag"])
def test_failure(dag_id):
    trigger_dag(dag_id)
    assert wait_for_dag(dag_id)


def trigger_dag(dag_id):
    r = requests.post(
        f"http://airflow:8080/api/v1/dags/{dag_id}/dagRuns",
        auth=HTTPBasicAuth('airflow', 'airflow'),
        json={},
        timeout=5,
    )
    r.raise_for_status()


if __name__ == "__main__":
    unittest.main()
