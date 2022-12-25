# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import sys
from typing import List

from pkg_resources import parse_version

import psycopg2
import time
import requests
from retrying import retry
import pytest
import unittest
from openlineage.common.test import match, setup_jinja

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
    creds = json.loads("/opt/config/gcloud/gcloud-service-key.json")
    if creds:
        IS_GCP_AUTH = True
except:  # noqa
    pass


SNOWFLAKE_AIRFLOW_TEST_VERSION = os.environ.get("SNOWFLAKE_AIRFLOW_TEST_VERSION", "2.2.4")


IS_AIRFLOW_VERSION_ENOUGH = lambda x: parse_version(
    os.environ.get("AIRFLOW_VERSION", "0.0.0")
) >= parse_version(x)


params = [
    ("postgres_orders_popular_day_of_week", "requests/postgres.json"),
    ("great_expectations_validation", "requests/great_expectations.json"),
    pytest.param(
        "bigquery_orders_popular_day_of_week",
        "requests/bigquery.json",
        marks=pytest.mark.skipif(not IS_GCP_AUTH, reason="no gcp credentials"),
    ),
    pytest.param(
        "dbt_bigquery",
        "requests/dbt_bigquery.json",
        marks=pytest.mark.skipif(not IS_GCP_AUTH, reason="no gcp credentials"),
    ),
    pytest.param(
        "athena_dag",
        "requests/athena.json",
        marks=pytest.mark.skipif(
            os.environ.get("AWS_ACCESS_KEY_ID", "") == "",
            reason="no aws credentials",
        ),
    ),
    ("source_code_dag", "requests/source_code.json"),
    pytest.param(
        "default_extractor_dag",
        "requests/default_extractor.json",
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.3.0"),  # both extract and extract_on_complete
            reason="Airflow < 2.3.0"                 # run on 2.3+
        )
    ),
    ("custom_extractor", "requests/custom_extractor.json"),
    ("unknown_operator_dag", "requests/unknown_operator.json"),
    pytest.param(
        "secrets",
        "requests/secrets.json",
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.1.0"),
            reason="Airflow < 2.1.0"
        )
    ),
    pytest.param(
        "mysql_orders_popular_day_of_week",
        "requests/mysql.json",
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.2.4"), reason="Airflow < 2.2.4"
        ),
    ),
    pytest.param(
        "trino_orders_popular_day_of_week",
        "requests/trino.json",
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.4.0"), reason="Airflow < 2.4.0"
        ),
    ),
    pytest.param(
        "dbt_snowflake",
        "requests/dbt_snowflake.json",
        marks=[
            pytest.mark.skipif(
                not IS_AIRFLOW_VERSION_ENOUGH(SNOWFLAKE_AIRFLOW_TEST_VERSION),
                reason=f"Airflow < {SNOWFLAKE_AIRFLOW_TEST_VERSION}",
            ),
            pytest.mark.skipif(
                os.environ.get("SNOWFLAKE_PASSWORD", "") == "",
                reason="no snowflake credentials",
            ),
        ],
    ),
    pytest.param(
        "snowflake",
        "requests/snowflake.json",
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
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.3.0"), reason="Airflow < 2.3.0"
        ),
    ),
    pytest.param(
        "task_group_dag",
        "requests/task_group.json",
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.3.0"), reason="Airflow < 2.3.0"
        ),
    ),
    ("sftp_dag", "requests/sftp.json"),
    pytest.param(
        "s3copy_dag",
        "requests/s3copy.json",
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.3.0"), reason="Airflow < 2.3.0"
        ),
    ),
    pytest.param(
        "s3transform_dag",
        "requests/s3transform.json",
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.3.0"), reason="Airflow < 2.3.0"
        ),
    ),
]


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=1000*10*60)
def wait_for_dag(dag_id, airflow_db_conn) -> bool:
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
    if state == "failed":
        return False
    elif state != "success":
        raise Exception("Retry!")
    return True


def check_matches(expected_events, actual_events) -> bool:
    for expected in expected_events:
        expected_job_name = env.from_string(expected["job"]["name"]).render()
        is_compared = False
        for actual in actual_events:
            # Try to find matching event by eventType and job name
            if (
                expected["eventType"] == actual["eventType"]
                and expected_job_name == actual["job"]["name"]
            ):
                is_compared = True
                if not match(expected, actual):
                    log.info(
                        f"failed to compare expected {expected}\nwith actual {actual}"
                    )
                    return False
                break
        if not is_compared:
            log.info(
                f"not found event comparable to {expected['eventType']} "
                f"- {expected_job_name}"
            )
            return False
    return True


def check_matches_ordered(expected_events, actual_events) -> bool:
    for index, expected in enumerate(expected_events):
        # Actual events have to be in the same order as expected events
        actual = actual_events[index]
        if (
            expected["eventType"] == actual["eventType"]
            and expected["job"]["name"] == actual["job"]["name"]
        ):
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
    r = requests.get(
        f"http://{backend_host}:5000/api/v1/lineage", params=params, timeout=5
    )
    r.raise_for_status()
    received_requests = r.json()
    return received_requests


@retry(wait_fixed=2000, stop_max_delay=15000)
def setup_db():
    host = os.environ.get("AIRFLOW_DB_HOST", "postgres")
    airflow_db_conn = psycopg2.connect(
        host=host, database="airflow", user="airflow", password="airflow"
    )
    airflow_db_conn.autocommit = True
    return airflow_db_conn


@pytest.fixture(scope="module", autouse=True)
def airflow_db_conn():
    yield setup_db()


@pytest.mark.parametrize("dag_id, request_path", params)
def test_integration(dag_id, request_path, airflow_db_conn):
    log.info(f"Checking dag {dag_id}")
    # (1) Wait for DAG to complete
    result = wait_for_dag(dag_id, airflow_db_conn)
    assert result is True

    # (2) Read expected events
    with open(request_path, "r") as f:
        expected_events = json.load(f)

    # (3) Get actual events
    actual_events = get_events()

    # (3) Verify events emitted
    log.info(f"failed to compare events for dag {dag_id}!")
    assert check_matches(expected_events, actual_events) is True
    log.info(f"Events for dag {dag_id} verified!")


@pytest.mark.parametrize("dag_id, request_dir, skip_jobs", [
    ("event_order", "requests/order", ['event_order']),
    pytest.param(
        "dag_event_order",
        "requests/dag_order",
        [],
        marks=pytest.mark.skipif(
            not IS_AIRFLOW_VERSION_ENOUGH("2.5.0rc2"), reason="Airflow <= 2.5.0rc2"
        ),
    )
])
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
        with open(os.path.join(request_dir, file), "r") as f:
            expected_events.append(json.load(f))

    # (3) Get actual events with job names starting with dag_id
    actual_events = get_events(dag_id, False)

    # (4) Filter jobs that we want to skip, which is
    #     used to skip dag events if we don't want to check them
    actual_events = [event for event in actual_events if event['job']['name'] not in skip_jobs]

    assert check_matches_ordered(expected_events, actual_events) is True
    assert check_event_time_ordered(actual_events) is True
    log.info(f"Events for dag {dag_id} verified!")


@pytest.mark.parametrize(
    "dag_id, request_path",
    [
        pytest.param(
            "mysql_orders_popular_day_of_week",
            "requests/airflow_run_facet/mysql.json",
            marks=pytest.mark.skipif(
                not IS_AIRFLOW_VERSION_ENOUGH("2.2.4"), reason="Airflow < 2.2.4"
            ),
        )
    ],
)
def test_airflow_run_facet(dag_id, request_path, airflow_db_conn):
    log.info(f"Checking dag {dag_id} for AirflowRunFacet")
    result = wait_for_dag(dag_id, airflow_db_conn)
    assert result is True

    with open(request_path, "r") as f:
        expected_events = json.load(f)

    actual_events = get_events()
    assert check_matches(expected_events, actual_events) is True


@pytest.mark.skipif(not IS_AIRFLOW_VERSION_ENOUGH("2.3.0"), reason="Airflow < 2.3.0")
def test_airflow_mapped_task_facet(airflow_db_conn):
    dag_id = "mapped_dag"
    task_id = "multiply"
    log.info(f"Checking dag {dag_id} for AirflowMappedTaskRunFacet")
    result = wait_for_dag(dag_id, airflow_db_conn)
    assert result is True

    actual_events = get_events(dag_id)
    mapped_events = [
        event
        for event in actual_events
        if event["job"]["name"] == f"{dag_id}.{task_id}"
    ]

    started_events = [event for event in mapped_events if event["eventType"] == "START"]
    assert len(started_events) == 3
    assert started_events[0]["job"]["name"] == f"{dag_id}.{task_id}"
    assert set(
        [event["job"]["facets"]["sourceCode"]["source"] for event in started_events]
    ) == set(["echo 1", "echo 2", "echo 3"])
    assert sorted(
        [
            event["run"]["facets"]["airflow_mappedTask"]["mapIndex"]
            for event in started_events
        ]
    ) == [0, 1, 2]
    assert set(
        [
            event["run"]["facets"]["airflow_mappedTask"]["operatorClass"]
            for event in started_events
        ]
    ) == set(["airflow.operators.bash.BashOperator"])
    assert set(
        [
            event["run"]["facets"]["unknownSourceAttribute"]["unknownItems"][0]["name"]
            for event in mapped_events
        ]
    ) == set(["BashOperator"])


if __name__ == "__main__":
    unittest.main()
