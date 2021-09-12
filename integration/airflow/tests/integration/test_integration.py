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
import sys
import psycopg2
import time
import requests
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
         WHERE dag_id = '{dag_id}';
        """
    )
    row = cur.fetchone()
    dag_id, state = row

    cur.close()

    log.info(f"DAG '{dag_id}' state set to '{state}'.")
    if state != "success":
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
                          f" not match: {len(v)} {len(request[k])}\n{expected}\n{request}")
                return False

            # Try to resolve case where we have wrongly sorted lists by looking at name attr
            # If name is not present then assume that lists are sorted
            for i, x in enumerate(v):
                if 'name' in x:
                    matched = False
                    for y in request[k]:
                        if 'name' in y and x['name'] == y['name']:
                            if not match(x, y):
                                return False
                            matched = True
                            break
                    if not matched:
                        return False
                else:
                    if not match(x, request[k][i]):
                        return False
        elif v != request[k]:
            log.error(f"For key {k}, value {v} not exquals {request[k]}"
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


def check_events_emitted(expected_requests):
    time.sleep(20)
    # Service in ./server captures requests and serves them
    r = requests.get('http://backend:5000/api/v1/lineage', timeout=5)
    r.raise_for_status()
    received_requests = r.json()

    return check_matches(expected_requests, received_requests)


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


def clear_db():
    requests.post('http://backend:5000/clear', timeout=5)


def test_integration_postgres():
    DAG_ID = 'postgres_orders_popular_day_of_week'

    # (0) Give db time to start
    setup_db()
    # (1) Wait for DAG to complete
    wait_for_dag(DAG_ID)
    # (2) Read expected events
    with open('requests/postgres.json', 'r') as f:
        expected_requests = json.load(f)

    # (3) Verify events emitted
    if not check_events_emitted(expected_requests):
        sys.exit(1)


def test_integration_bigquery():
    DAG_ID = 'bigquery_orders_popular_day_of_week'

    # (0) Give db time to start
    setup_db()
    # (1) Wait for DAG to complete
    wait_for_dag(DAG_ID)
    # (2) Read expected events
    with open('requests/bigquery.json', 'r') as f:
        expected_requests = json.load(f)

    # (3) Verify events emitted
    if not check_events_emitted(expected_requests):
        sys.exit(1)


def test_integration_great_expectations():
    DAG_ID = 'great_expectations_validation'

    # (1) Wait for DAG to complete
    wait_for_dag(DAG_ID)
    # (2) Read expected events
    with open('requests/great_expectations.json', 'r') as f:
        expected_requests = json.load(f)

    time.sleep(10)

    # (3) Verify events emitted
    if not check_events_emitted(expected_requests):
        sys.exit(1)


if __name__ == '__main__':
    setup_db()
    test_integration_great_expectations()
    test_integration_postgres()
    test_integration_bigquery()
