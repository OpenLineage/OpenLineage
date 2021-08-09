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
import logging
import time
import json
import sys

import requests
import psycopg2

from airflow.utils.state import State as DagState

from retrying import retry


logging.basicConfig(
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    level='DEBUG'
)
log = logging.getLogger(__name__)

NAMESPACE_NAME = 'food_delivery'

# AIRFLOW
DAG_ID = 'orders_popular_day_of_week'

airflow_db_conn = None


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


def match(expected, request):
    for k, v in expected.items():
        if k not in request:
            log.error(f"Key {k} not in event {request}\nExpected {expected}")
            return False
        elif isinstance(v, dict):
            if not match(v, request[k]):
                return False
        elif isinstance(v, list):
            if len(v) != len(request[k]):
                log.error(f"For list of key {k}, length of lists does"
                             f" not match: {len(v)} {len(request[k])}")
                return False
            if not all([match(x, y) for x, y in zip(v, request[k])]):
                return False
        elif v != request[k]:
            log.error(f"For key {k}, value {v} not in event {request[k]}"
                         f"\nExpected {expected}, request {request}")
            return False
    return True


def check_matches(expected_requests, received_requests):
    for expected in expected_requests:
        is_compared = False
        for request in received_requests:
            if expected['eventType'] == request['eventType'] and \
                    expected['job']['name'] == request['job']['name']:
                is_compared = True
                if not match(expected, request):
                    return False
                break
        if not is_compared:
            log.info(f"not found event comparable to {expected['eventType']} "
                        f"- {expected['job']['name']}")
            return False
    return True


def check_events_emitted():

    with open('expected_requests.json', 'r') as f:
        expected_requests = json.load(f)

    time.sleep(20)
    # Service in ./server does the checking
    r = requests.get('http://backend:5000/api/v1/lineage', timeout=5)
    r.raise_for_status()
    received_requests = r.json()

    check_matches(expected_requests, received_requests)


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


def main():
    # (0) Give db time to start
    setup_db()
    # (1) Wait for DAG to complete
    wait_for_dag()
    # (2) Verify events emitted
    check_events_emitted()


if __name__ == "__main__":
    main()
