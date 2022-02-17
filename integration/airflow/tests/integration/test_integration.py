# SPDX-License-Identifier: Apache-2.0.
import json
import logging
import os
import sys

import psycopg2
import time
import requests
from retrying import retry

from openlineage.common.test import match

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
         WHERE dag_id = '{dag_id}';
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


def check_matches(expected_events, actual_events) -> bool:
    for expected in expected_events:
        is_compared = False
        for actual in actual_events:
            # Try to find matching event by eventType and job name
            if expected['eventType'] == actual['eventType'] and \
                    expected['job']['name'] == actual['job']['name']:
                is_compared = True
                if not match(expected, actual):
                    log.info(f"failed to compare expected {expected}\nwith actual {actual}")
                    return False
                break
        if not is_compared:
            log.info(f"not found event comparable to {expected['eventType']} "
                     f"- {expected['job']['name']}")
            return False
    return True


def check_matches_ordered(expected_events, actual_events) -> bool:
    for index, expected in enumerate(expected_events):
        # Actual events have to be in the same order as expected events
        actual = actual_events[index]
        if expected['eventType'] == actual['eventType'] and \
                expected['job']['name'] == actual['job']['name']:
            if not match(expected, actual):
                log.info(f"failed to compare expected {expected}\nwith actual {actual}")
                return False
            break
        log.info(f"Wrong order of events: expected {expected['eventType']} - "
                 f"{expected['job']['name']}\ngot {actual['eventType']} - {actual['job']['name']}")
        return False
    return True


def check_event_time_ordered(actual_events) -> bool:
    event_times = [event['eventTime'] for event in actual_events]

    # Check event times are not all same and properly ordered
    if len(set(event_times)) != len(event_times) or \
            event_times != sorted(event_times):
        return False
    return True


def get_events(job_name: str = None):
    time.sleep(5)
    params = {}

    if job_name:
        params = {
            "job_name": job_name
        }

    # Service in ./server captures requests and serves them
    r = requests.get(
        'http://backend:5000/api/v1/lineage',
        params=params,
        timeout=5
    )
    r.raise_for_status()
    received_requests = r.json()
    return received_requests


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


def test_integration(dag_id, request_path):
    log.info(f"Checking dag {dag_id}")
    # (1) Wait for DAG to complete
    if not wait_for_dag(dag_id):
        sys.exit(1)
    # (2) Read expected events
    with open(request_path, 'r') as f:
        expected_events = json.load(f)

    # (3) Get actual events
    actual_events = get_events()

    # (3) Verify events emitted
    if not check_matches(expected_events, actual_events):
        log.info(f"failed to compare events!")
        sys.exit(1)


def test_integration_ordered(dag_id, request_dir: str):
    log.info(f"Checking dag {dag_id}")
    # (1) Wait for DAG to complete
    if not wait_for_dag(dag_id):
        sys.exit(1)
    # (2) Find and read events in given directory on order of file names.
    #     The events have to arrive at the server in the same order.
    event_files = sorted(
        os.listdir(request_dir),
        key=lambda x: int(x.split('.')[0])
    )
    expected_events = []
    for file in event_files:
        with open(os.path.join(request_dir, file), 'r') as f:
            expected_events.append(json.load(f))

    # (3) Get actual events with job names starting with dag_id
    actual_events = get_events(dag_id)

    if not check_matches_ordered(expected_events, actual_events):
        log.info(f"failed to compare events!")
        sys.exit(1)

    if not check_event_time_ordered(actual_events):
        log.info(f"failed on event timestamp order!")
        sys.exit(1)


if __name__ == '__main__':
    setup_db()
    test_integration('postgres_orders_popular_day_of_week', 'requests/postgres.json')
    test_integration('great_expectations_validation', 'requests/great_expectations.json')
    test_integration('bigquery_orders_popular_day_of_week', 'requests/bigquery.json')
    test_integration('dbt_dag', 'requests/dbt.json')
    test_integration('source_code_dag', 'requests/source_code.json')
    test_integration('custom_extractor', 'requests/custom_extractor.json')
    test_integration_ordered('event_order', 'requests/order')
