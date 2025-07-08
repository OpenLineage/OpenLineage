# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import sys
import time
import unittest
from collections import defaultdict
from typing import Dict, List

import psycopg2
import pytest
import requests
from openlineage.client.event_v2 import RunState
from openlineage.common.test import match, setup_jinja
from openlineage.common.utils import get_from_nullable_chain
from packaging.version import Version
from retrying import retry

env = setup_jinja()

logging.basicConfig(
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    level="DEBUG",
)
log = logging.getLogger(__name__)

IS_GCP_AUTH = False
try:
    with open("/opt/config/gcloud/gcloud-service-key.json") as f:
        if json.load(f):
            IS_GCP_AUTH = True
except Exception:
    pass


SNOWFLAKE_AIRFLOW_TEST_VERSION = os.environ.get("SNOWFLAKE_AIRFLOW_TEST_VERSION", "2.6.1")


def IS_AIRFLOW_VERSION_ENOUGH(x):
    return Version(os.environ.get("AIRFLOW_VERSION", "0.0.0")) >= Version(x)


params = [
    ("postgres_orders_popular_day_of_week", "requests/postgres.json", True),
    ("failed_sql_extraction", "requests/failed_sql_extraction.json", True),
    ("great_expectations_validation", "requests/great_expectations.json", True),
    pytest.param(
        "bigquery_orders_popular_day_of_week",
        "requests/bigquery.json",
        True,
        marks=pytest.mark.skipif(not IS_GCP_AUTH, reason="no gcp credentials"),
    ),
    pytest.param(
        "gcs_dag",
        "requests/gcs.json",
        True,
        marks=[
            pytest.mark.skipif(not IS_GCP_AUTH, reason="no gcp credentials"),
            pytest.mark.skipif(
                os.environ.get("GOOGLE_CLOUD_STORAGE_SOURCE_URI") == "",
                reason="no gcs source uri",
            ),
            pytest.mark.skipif(
                os.environ.get("GOOGLE_CLOUD_STORAGE_DESTINATION_URI") == "",
                reason="no gcs destination uri",
            ),
        ],
    ),
    pytest.param(
        "athena_dag",
        "requests/athena.json",
        True,
        marks=pytest.mark.skipif(
            os.environ.get("AWS_ACCESS_KEY_ID", "") == "",
            reason="no aws credentials",
        ),
    ),
    ("source_code_dag", "requests/source_code.json", True),
    pytest.param("default_extractor_dag", "requests/default_extractor.json", True),
    ("custom_extractor", "requests/custom_extractor.json", True),
    ("unknown_operator_dag", "requests/unknown_operator.json", True),
    pytest.param("secrets", "requests/secrets.json", True),
    pytest.param("mysql_orders_popular_day_of_week", "requests/mysql.json", True),
    pytest.param(
        "trino_orders_popular_day_of_week",
        "requests/trino.json",
        True,
    ),
    pytest.param(
        "snowflake",
        "requests/snowflake.json",
        True,
        marks=[
            pytest.mark.skipif(
                not IS_AIRFLOW_VERSION_ENOUGH(SNOWFLAKE_AIRFLOW_TEST_VERSION),
                reason=f"Airflow < {SNOWFLAKE_AIRFLOW_TEST_VERSION}",
            ),
            pytest.mark.skipif(
                os.environ.get("SNOWFLAKE_ACCOUNT_ID", "") == "",
                reason="no snowflake credentials",
            ),
        ],
    ),
    pytest.param(
        "mapped_dag",
        "requests/mapped_dag.json",
        False,
    ),
    pytest.param("task_group_dag", "requests/task_group.json", False),
    ("sftp_dag", "requests/sftp.json", True),
    pytest.param(
        "ftp_dag",
        "requests/ftp.json",
        True,
    ),
    pytest.param("s3copy_dag", "requests/s3copy.json", True),
    pytest.param("s3transform_dag", "requests/s3transform.json", True),
]


@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_delay=1000 * 10 * 60,
)
def wait_for_dag(dag_id, airflow_db_conn, should_fail=False) -> bool:
    log.info(f"Waiting for DAG '{dag_id}'...")

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
    expected_state = "failed" if should_fail else "success"
    failing_state = "success" if should_fail else "failed"
    if state == failing_state:
        return False
    elif state != expected_state:
        raise Exception("Retry!")
    return True


def check_run_id_matches(actual_events: List[Dict]) -> bool:
    """Checks if the events have matching `runId` values for 'START' and 'COMPLETE'/'FAIL' states.

    Each `runId` should have exactly one 'START' event and one 'COMPLETE' or 'FAIL' event.
    If an unknown event type is encountered or the counts of event types do not match the expected pattern,
    the function returns `False`.

    :param actual_events: A list of event dictionaries.

    :return: True if all `runId`s have matching event counts, False otherwise.

    Examples:
    >>> check_run_id_matches([
    ...     {"eventType": "START", "run": {"runId": "run123"}},
    ...     {"eventType": "COMPLETE", "run": {"runId": "run123"}}
    ... ])
    True

    >>> check_run_id_matches([
    ...     {"eventType": "START", "run": {"runId": "run123"}},
    ... ])
    False

    >>> check_run_id_matches([
    ...     {"eventType": "START", "run": {"runId": "run123"}},
    ...     {"eventType": "COMPLETE", "run": {"runId": "run123"}},
    ...     {"eventType": "FAIL", "run": {"runId": "run123"}}
    ... ])
    False

    >>> mapped_task_facet = {"airflow": {"task": {"mapped": True}}}
    >>> check_run_id_matches([
    ...     {"eventType": "START", "run": {"runId": "run123", "facets": mapped_task_facet}},
    ... ])
    False

    >>> mapped_task_facet = {"airflow": {"task": {"mapped": True}}}
    >>> check_run_id_matches([
    ...     {"eventType": "START", "run": {"runId": "run123", "facets": mapped_task_facet}},
    ...     {"eventType": "COMPLETE", "run": {"runId": "run123", "facets": mapped_task_facet}},
    ...     {"eventType": "START", "run": {"runId": "run123", "facets": mapped_task_facet}},
    ...     {"eventType": "FAIL", "run": {"runId": "run123", "facets": mapped_task_facet}},
    ... ])
    True

    >>> check_run_id_matches([
    ...     {"eventType": "START", "run": {"runId": "run123"}},
    ...     {"eventType": "COMPLETE", "run": {"runId": "run123"}},
    ...     {"eventType": "FAIL", "run": {"runId": "run123"}}
    ... ])
    False
    """
    start = RunState.START.value
    complete = RunState.COMPLETE.value
    fail = RunState.FAIL.value

    event_tracker = defaultdict(lambda: {start: 0, complete: 0, fail: 0, "is_mapped": False})

    # Process each event in the list
    for event in actual_events:
        event_type = event["eventType"]
        event_run_id = event["run"]["runId"]

        is_dynamic_task = get_from_nullable_chain(event, ["run", "facets", "airflow", "task", "mapped"])
        if is_dynamic_task is not None:
            event_tracker[event_run_id]["is_mapped"] = is_dynamic_task

        if event_type in [start, complete, fail]:
            event_tracker[event_run_id][event_type] += 1
        else:
            log.info(f"Found unknown event_type: `{event_type}` for run_id: `{event_run_id}`")
            return False

    # Validate each run_id
    failed_runs = {}
    for run_id, events in event_tracker.items():
        log.debug(f"{run_id} -> {events}")
        if events["is_mapped"]:
            # Dynamic tasks will have the same run_id, so we can allow more than one start and complete
            if events[start] != (events[complete] + events[fail]):
                failed_runs[run_id] = events
        else:
            if events[start] != 1 or (events[complete] + events[fail]) != 1:
                failed_runs[run_id] = events

    if failed_runs:
        log_msg = "Validation failed for the following run_ids:\n"
        for run_id, events in failed_runs.items():
            log_msg += f"\t\t{run_id} -> {events}\n"
        log.info(log_msg)
        return False
    return True


def check_matches(expected_events, actual_events, check_duplicates: bool = None) -> bool:
    for expected in expected_events:
        expected_job_name = env.from_string(expected["job"]["name"]).render()
        is_compared = False
        for actual in actual_events:
            # Try to find matching event by eventType and job name
            if expected["eventType"] == actual["eventType"] and expected_job_name == actual["job"]["name"]:
                if is_compared and check_duplicates:
                    log.info(f"found more than one event expected {expected}\n duplicate: {actual}")
                    return False
                is_compared = True
                if not match(expected, actual):
                    log.info(f"failed to compare expected {expected}\nwith actual {actual}")
                    return False
        if not is_compared:
            log.info(f"not found event comparable to {expected['eventType']} " f"- {expected_job_name}")
            return False
    return True


def check_matches_ordered(expected_events, actual_events) -> bool:
    for index, expected in enumerate(expected_events):
        # Actual events have to be in the same order as expected events
        actual = actual_events[index]
        if expected["eventType"] == actual["eventType"] and expected["job"]["name"] == actual["job"]["name"]:
            if not match(expected, actual):
                log.info(f"failed to compare expected {expected}\nwith actual {actual}")
                return False
            break
        log.info(
            f"Wrong order of events: expected {expected['eventType']} - "
            f"{expected['job']['name']}\ngot {actual['eventType']} - {actual['job']['name']}"
        )
        return False
    return True


def check_event_time_ordered(actual_events) -> bool:
    event_times = [event["eventTime"] for event in actual_events]

    # Check event times are not all same and properly ordered
    if len(set(event_times)) != len(event_times) or event_times != sorted(event_times):
        return False
    return True


def get_events(job_name: str = None, desc: bool = True):
    time.sleep(5)
    params = {}

    if job_name:
        params = {"job_name": job_name, "desc": "true" if desc else "false"}

    # Service in ./server captures requests and serves them
    backend_host = os.environ.get("BACKEND_HOST", "backend")
    r = requests.get(f"http://{backend_host}:5000/api/v1/lineage", params=params, timeout=5)
    r.raise_for_status()
    received_requests = r.json()
    return received_requests


@retry(wait_fixed=2000, stop_max_delay=15000)
def setup_db():
    host = os.environ.get("AIRFLOW_DB_HOST", "postgres")
    airflow_db_conn = psycopg2.connect(host=host, database="airflow", user="airflow", password="airflow")
    airflow_db_conn.autocommit = True
    return airflow_db_conn


@pytest.fixture(scope="module", autouse=True)
def airflow_db_conn():
    return setup_db()


@pytest.mark.parametrize("dag_id, request_path, check_duplicates", params)
def test_integration(dag_id, request_path, check_duplicates, airflow_db_conn):
    log.info(f"Checking dag {dag_id}")
    # (1) Wait for DAG to complete
    result = wait_for_dag(dag_id, airflow_db_conn)
    assert result is True

    # (2) Read expected events
    with open(request_path) as f:
        expected_events = json.load(f)

    # (3) Get actual events
    actual_events = get_events()

    # (4) Verify run_id is constant across pairs of actual events for this dag
    dag_events = [x for x in actual_events if dag_id in x["job"]["name"]]
    assert check_run_id_matches(dag_events) is True

    # (5) Verify events emitted
    assert check_matches(expected_events, actual_events, check_duplicates) is True
    log.info(f"Events for dag {dag_id} verified!")


@pytest.mark.parametrize(
    "dag_id, request_path, check_duplicates",
    [
        pytest.param("async_dag", "requests/failing/async.json", True),
        pytest.param("bash_dag", "requests/failing/bash.json", True),
    ],
)
def test_failing_dag(dag_id, request_path, check_duplicates, airflow_db_conn):
    log.info(f"Checking dag {dag_id}")
    # (1) Wait for DAG to complete
    result = wait_for_dag(dag_id, airflow_db_conn, should_fail=True)
    assert result is True

    # (2) Read expected events
    with open(request_path) as f:
        expected_events = json.load(f)

    # (3) Get actual events
    actual_events = get_events()

    # (4) Verify run_id is constant across pairs of actual events for this dag
    dag_events = [x for x in actual_events if dag_id in x["job"]["name"]]
    assert check_run_id_matches(dag_events) is True

    # (5) Verify events emitted
    assert check_matches(expected_events, actual_events, check_duplicates) is True
    log.info(f"Events for dag {dag_id} verified!")


@pytest.mark.parametrize(
    "dag_id, request_dir, skip_jobs",
    [
        ("event_order", "requests/order", ["event_order"]),
        pytest.param(
            "dag_event_order",
            "requests/dag_order",
            [],
        ),
    ],
)
def test_integration_ordered(dag_id, request_dir: str, skip_jobs: List[str], airflow_db_conn):
    log.info(f"Checking dag {dag_id}")
    # (1) Wait for DAG to complete
    result = wait_for_dag(dag_id, airflow_db_conn)
    assert result is True

    # (2) Find and read events in given directory on order of file names.
    #     The events have to arrive at the server in the same order.
    event_files = sorted(os.listdir(request_dir), key=lambda x: int(x.split(".")[0]))
    expected_events = []
    for file in event_files:
        with open(os.path.join(request_dir, file)) as f:
            expected_events.append(json.load(f))

    # (3) Get actual events with job names starting with dag_id
    actual_events = get_events(dag_id, False)

    # # (4) Verify run_id is constant across pairs of actual events
    assert check_run_id_matches(actual_events) is True

    # (5) Filter jobs that we want to skip, which is
    #     used to skip dag events if we don't want to check them
    actual_events = [event for event in actual_events if event["job"]["name"] not in skip_jobs]

    assert check_matches_ordered(expected_events, actual_events) is True
    assert check_event_time_ordered(actual_events) is True
    log.info(f"Events for dag {dag_id} verified!")


@pytest.mark.parametrize(
    "dag_id, request_path",
    [
        pytest.param(
            "mysql_orders_popular_day_of_week",
            "requests/airflow_run_facet/mysql.json",
        )
    ],
)
def test_airflow_run_facet(dag_id, request_path, airflow_db_conn):
    log.info(f"Checking dag {dag_id} for AirflowRunFacet")
    result = wait_for_dag(dag_id, airflow_db_conn)
    assert result is True

    with open(request_path) as f:
        expected_events = json.load(f)

    actual_events = get_events()
    assert check_matches(expected_events, actual_events) is True


def test_airflow_mapped_task_facet(airflow_db_conn):
    dag_id = "mapped_dag"
    task_id = "multiply"
    log.info(f"Checking dag {dag_id} for AirflowMappedTaskRunFacet")
    result = wait_for_dag(dag_id, airflow_db_conn)
    assert result is True

    actual_events = get_events(dag_id)
    mapped_events = [event for event in actual_events if event["job"]["name"] == f"{dag_id}.{task_id}"]

    started_events = [event for event in mapped_events if event["eventType"] == "START"]
    assert len(started_events) == 3
    assert started_events[0]["job"]["name"] == f"{dag_id}.{task_id}"
    assert set([event["job"]["facets"]["sourceCode"]["sourceCode"] for event in started_events]) == set(
        ["echo 1", "echo 2", "echo 3"]
    )
    assert sorted([event["run"]["facets"]["airflow_mappedTask"]["mapIndex"] for event in started_events]) == [
        0,
        1,
        2,
    ]
    assert set(
        [event["run"]["facets"]["airflow_mappedTask"]["operatorClass"] for event in started_events]
    ) == set(["airflow.operators.bash.BashOperator"])
    assert set(
        [
            event["run"]["facets"]["unknownSourceAttribute"]["unknownItems"][0]["name"]
            for event in mapped_events
        ]
    ) == set(["BashOperator"])


if __name__ == "__main__":
    unittest.main()
