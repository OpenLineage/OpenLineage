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
import os
import subprocess
from uuid import uuid4
from urllib.parse import urlparse, urlunparse

import airflow
from airflow.models import Connection
from airflow.utils.db import provide_session
from marquez_airflow.facets import AirflowVersionRunFacet, \
    AirflowRunArgsRunFacet

try:
    # Import from pendulum 1.x version
    from pendulum import Pendulum, from_timestamp
except ImportError:
    # Import for Pendulum 2.x version
    from pendulum import DateTime as Pendulum, from_timestamp

log = logging.getLogger(__name__)
_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class JobIdMapping:
    # job_name here is marquez job name - aka combination of dag_id and task_id

    @staticmethod
    def set(job_name: str, dag_run_id: str, task_run_id: str):
        airflow.models.Variable.set(
            JobIdMapping.make_key(job_name, dag_run_id),
            json.dumps(task_run_id)
        )

    @staticmethod
    def pop(job_name, dag_run_id, session):
        return JobIdMapping.get(job_name, dag_run_id, session, delete=True)

    @staticmethod
    def get(job_name, dag_run_id, session, delete=False):
        key = JobIdMapping.make_key(job_name, dag_run_id)
        if session:
            q = session.query(airflow.models.Variable).filter(
                airflow.models.Variable.key == key)
            if not q.first():
                return None
            else:
                val = q.first().val
                if delete:
                    q.delete(synchronize_session=False)
                if val:
                    return json.loads(val)
                return None

    @staticmethod
    def make_key(job_name, run_id):
        return "marquez_id_mapping-{}-{}".format(job_name, run_id)


def url_to_https(url) -> str:
    # Ensure URL exists
    if not url:
        return None

    base_url = None
    if url.startswith('git@'):
        part = url.split('git@')[1:2]
        if part:
            base_url = f'https://{part[0].replace(":", "/", 1)}'
    elif url.startswith('https://'):
        base_url = url

    if not base_url:
        raise ValueError(f"Unable to extract location from: {url}")

    if base_url.endswith('.git'):
        base_url = base_url[:-4]
    return base_url


def get_location(file_path) -> str:
    # Ensure file path exists
    if not file_path:
        return None

    # move to the file directory
    abs_path = os.path.abspath(file_path)
    file_name = os.path.basename(file_path)
    cwd = os.path.dirname(abs_path)

    # get the repo url
    repo_url = execute_git(cwd, ['config', '--get', 'remote.origin.url'])

    # get the repo relative path
    repo_relative_path = execute_git(cwd, ['rev-parse', '--show-prefix'])

    # get the commitId for the particular file
    commit_id = execute_git(cwd, ['rev-list', 'HEAD', '-1', '--', file_name])

    # build the URL
    base_url = url_to_https(repo_url)
    if not base_url:
        return None

    return f'{base_url}/blob/{commit_id}/{repo_relative_path}{file_name}'


def execute_git(cwd, params):
    p = subprocess.Popen(['git'] + params,
                         cwd=cwd, stdout=subprocess.PIPE, stderr=None)
    p.wait(timeout=0.5)
    out, err = p.communicate()
    return out.decode('utf8').strip()


def get_connection_uri(conn: Connection):
    """
    Return the connection URI for the given ID. We first attempt to lookup
    the connection URI via AIRFLOW_CONN_<conn_id>, else fallback on querying
    the Airflow's connection table.
    """

    conn_uri = conn.get_uri()
    parsed = urlparse(conn_uri)

    # Remove username and password
    parsed = parsed._replace(netloc=f'{parsed.hostname}:{parsed.port}')
    return urlunparse(parsed)


def get_normalized_postgres_connection_uri(conn: Connection):
    """
    URIs starting with postgresql:// and postgres:// are both valid
    PostgreSQL connection strings. This function normalizes it to
    postgres:// as canonical name according to OpenLineage spec.
    """
    uri = get_connection_uri(conn)
    if uri.startswith('postgresql'):
        uri = uri.replace('postgresql', 'postgres', 1)
    return uri


@provide_session
def get_connection(conn_id, session=None) -> Connection:
    # TODO: We may want to throw an exception if the connection
    # does not exist (ex: AirflowConnectionException). The connection
    # URI is required when collecting metadata for a data source.
    conn_uri = os.environ.get('AIRFLOW_CONN_' + conn_id.upper())
    if conn_uri:
        conn = Connection()
        conn.parse_from_uri(conn_uri)
        return conn

    return (session
            .query(Connection)
            .filter(Connection.conn_id == conn_id)
            .first())


def get_job_name(task):
    return f'{task.dag_id}.{task.task_id}'


def get_custom_facets(task, is_external_trigger: bool):
    return {
        "airflow_runArgs": AirflowRunArgsRunFacet(is_external_trigger),
        "airflow_version": AirflowVersionRunFacet.from_task(task)
    }


def new_lineage_run_id(dag_run_id: str, task_id: str) -> str:
    return str(uuid4())


class DagUtils:

    def get_execution_date(**kwargs):
        return kwargs.get('execution_date')

    @staticmethod
    def get_start_time(execution_date=None):
        if execution_date:
            return DagUtils.to_iso_8601(execution_date)
        else:
            return None

    @staticmethod
    def get_end_time(execution_date, default):
        if execution_date:
            end_time = default
        else:
            end_time = None

        if end_time:
            end_time = DagUtils.to_iso_8601(end_time)
        return end_time

    @staticmethod
    def to_iso_8601(dt):
        if not dt:
            return None
        if isinstance(dt, int):
            dt = from_timestamp(dt/1000.0)

        if isinstance(dt, Pendulum):
            return dt.format(_NOMINAL_TIME_FORMAT)
        else:
            return dt.strftime(_NOMINAL_TIME_FORMAT)
