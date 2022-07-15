# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import importlib
import json
import logging
import os
import subprocess
from collections import defaultdict
from typing import TYPE_CHECKING, Type, Dict, Any, List
from uuid import uuid4
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import Optional

from airflow.version import version as AIRFLOW_VERSION

from pkg_resources import parse_version

from openlineage.airflow.facets import (
    AirflowMappedTaskRunFacet,
    AirflowVersionRunFacet,
    AirflowRunArgsRunFacet
)
from openlineage.client.facet import (
    DataQualityMetricsInputDatasetFacet,
    ColumnMetric,
    DataQualityAssertionsDatasetFacet,
    Assertion
)
from pendulum import from_timestamp


if TYPE_CHECKING:
    from airflow.models import Connection, BaseOperator, TaskInstance


log = logging.getLogger(__name__)
_NOMINAL_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def openlineage_job_name(dag_id: str, task_id: str) -> str:
    return f'{dag_id}.{task_id}'


def get_operator_class(task: "BaseOperator") -> Type:
    if task.__class__.__name__ in ('DecoratedMappedOperator', 'MappedOperator'):
        return task.operator_class
    return task.__class__


class JobIdMapping:
    # job_name here is OL job name - aka combination of dag_id and task_id

    @staticmethod
    def set(job_name: str, dag_run_id: str, task_run_id: str):
        from airflow.models import Variable
        Variable.set(
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
            from airflow.models import Variable
            q = session.query(Variable).filter(Variable.key == key)
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
        return "openlineage_id_mapping-{}-{}".format(job_name, run_id)


class SafeStrDict(dict):
    def __str__(self):
        castable = list()
        for attr, val in self.items():
            try:
                str(attr), str(val)
                castable.append((attr, val))
            except (TypeError, NotImplementedError):
                continue
        return str(dict(castable))


def url_to_https(url) -> Optional[str]:
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


def get_location(file_path) -> Optional[str]:
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


def get_task_location(task):
    try:
        if hasattr(task, 'file_path') and task.file_path:
            return get_location(task.file_path)
        else:
            return get_location(task.dag.fileloc)
    except Exception:
        return None


def execute_git(cwd, params):
    p = subprocess.Popen(['git'] + params,
                         cwd=cwd, stdout=subprocess.PIPE, stderr=None)
    p.wait(timeout=0.5)
    out, err = p.communicate()
    return out.decode('utf8').strip()


def get_connection_uri(conn):
    """
    Return the connection URI for the given ID. We first attempt to lookup
    the connection URI via AIRFLOW_CONN_<conn_id>, else fallback on querying
    the Airflow's connection table.
    """

    conn_uri = conn.get_uri()
    parsed = urlparse(conn_uri)

    # Remove username and password
    netloc = f'{parsed.hostname}' + (f':{parsed.port}' if parsed.port else "")
    parsed = parsed._replace(netloc=netloc)
    query_dict = parse_qs(parsed.query)
    filtered_qs = {k: query_dict[k]
                   for k in query_dict.keys()
                   if not _filtered_query_params(k)}
    parsed = parsed._replace(query=urlencode(filtered_qs))
    return urlunparse(parsed)


def _filtered_query_params(k: str):
    unfiltered_snowflake_keys = ["extra__snowflake__warehouse",
                                 "extra__snowflake__account",
                                 "extra__snowflake__database"]
    filtered_key_substrings = ["aws_access_key_id",
                               "aws_secret_access_key",
                               "extra__snowflake__"]
    return k not in unfiltered_snowflake_keys and \
        any(substr in k for substr in filtered_key_substrings)


def get_normalized_postgres_connection_uri(conn):
    """
    URIs starting with postgresql:// and postgres:// are both valid
    PostgreSQL connection strings. This function normalizes it to
    postgres:// as canonical name according to OpenLineage spec.
    """
    uri = get_connection_uri(conn)
    if uri.startswith('postgresql'):
        uri = uri.replace('postgresql', 'postgres', 1)
    return uri


def get_connection(conn_id) -> "Connection":
    # TODO: We may want to throw an exception if the connection
    # does not exist (ex: AirflowConnectionException). The connection
    # URI is required when collecting metadata for a data source.
    from airflow.models import Connection
    conn_uri = os.environ.get('AIRFLOW_CONN_' + conn_id.upper())
    if conn_uri:
        conn = Connection()
        conn.parse_from_uri(uri=conn_uri)
        return conn

    # Airflow 2: use secrets backend.
    if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):    # type: ignore
        try:
            return Connection.get_connection_from_secrets(conn_id)
        except Exception:
            # Deliberate pass to try getting it via query
            pass

    create_session = safe_import_airflow(
        airflow_1_path="airflow.utils.db.create_session",
        airflow_2_path="airflow.utils.session.create_session",
    )

    with create_session() as session:
        return session.query(Connection)\
            .filter(Connection.conn_id == conn_id)\
            .first()


def get_job_name(task):
    return f'{task.dag_id}.{task.task_id}'


def get_custom_facets(
    task, is_external_trigger: bool, task_instance: "TaskInstance" = None
) -> Dict[str, Any]:
    custom_facets = {
        "airflow_runArgs": AirflowRunArgsRunFacet(is_external_trigger),
        "airflow_version": AirflowVersionRunFacet.from_task(task),
    }
    # check for -1 comes from SmartSensor compatibility with dynamic task mapping
    # this comes from Airflow code
    if hasattr(task_instance, "map_index") and getattr(task_instance, "map_index") != -1:
        custom_facets["airflow_mappedTask"] = AirflowMappedTaskRunFacet.from_task_instance(
            task_instance
        )
    return custom_facets


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
            dt = from_timestamp(dt / 1000.0)

        return dt.strftime(_NOMINAL_TIME_FORMAT)


def import_from_string(path: str):
    try:
        module_path, target = path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, target)
    except Exception as e:
        raise ImportError(f"Failed to import {path}") from e


def try_import_from_string(path: str):
    try:
        return import_from_string(path)
    except ImportError as e:
        logging.info(e.msg)     # type: ignore
        return None


def choose_based_on_version(airflow_1_version, airflow_2_version):
    if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
        return airflow_2_version
    else:
        return airflow_1_version


def safe_import_airflow(airflow_1_path: str, airflow_2_path: str):
    return import_from_string(
        choose_based_on_version(
            airflow_1_path, airflow_2_path
        )
    )


def build_check_facets() -> dict:
    pass


def build_value_check_facets() -> dict:
    pass


def build_threshold_check_facets() -> dict:
    pass


def build_interval_check_facets() -> dict:
    pass

def build_table_check_facets(checks) -> dict:
    """
    Function should expect to take the checks in the following form:
    {
        'row_count_check': {
            'pass_value': 100,
            'tolerance': .05,
            'result': 101,
            'success': True
        }
    }
    """
    facet_data = {}
    assertion_data: Dict[str, List[Assertion]] = {"assertions": []}
    for check, check_values in checks.items():
        assertion_data["assertions"].append(
            Assertion(
                assertion=check,
                success=check_values.get("success", None),
            )
        )
    facet_data["rowCount"] = checks.get("row_count_check", {}).get("result", None)
    facet_data["bytes"] = checks.get("bytes", {}).get("result", None)

    data_quality_facet = DataQualityMetricsInputDatasetFacet(**facet_data)
    data_quality_assertions_facet = DataQualityAssertionsDatasetFacet(**assertion_data)

    return {
        "dataQuality": data_quality_facet,
        "dataQualityMetrics": data_quality_facet,
        "dataQualityAssertions": data_quality_assertions_facet
    }


def build_column_check_facets(column_mapping) -> dict:
    """
    Function should expect the column_mapping to take the following form:
    {
        'col_name': {
            'null_check': {
                'pass_value': 0,
                'result': 0,
                'success': True
            },
            'min': {
                'pass_value': 5,
                'tolerance': 0.2,
                'result': 1,
                'success': False
            }
        }
    }
    """
    facet_data: Dict[str, Any] = {"columnMetrics": defaultdict(dict)}
    assertion_data: Dict[str, List[Assertion]] = {"assertions": []}
    for col_name, checks in column_mapping.items():
        for check, check_values in checks.items():
            facet_key = map_facet_name(check)
            facet_data["columnMetrics"][col_name][facet_key] = check_values.get("result", None)

            assertion_data["assertions"].append(
                Assertion(
                    assertion=check,
                    success=check_values.get("success", None),
                    column=col_name
                )
            )
        facet_data["columnMetrics"][col_name] = ColumnMetric(
            **facet_data["columnMetrics"][col_name]
        )

    data_quality_facet = DataQualityMetricsInputDatasetFacet(**facet_data)
    data_quality_assertions_facet = DataQualityAssertionsDatasetFacet(**assertion_data)

    return {
        "dataQuality": data_quality_facet,
        "dataQualityMetrics": data_quality_facet,
        "dataQualityAssertions": data_quality_assertions_facet
    }


def map_facet_name(check_name) -> str:
    if "null" in check_name:
        return "nullCount"
    elif "distinct" in check_name:
        return "distinctCount"
    elif "sum" in check_name:
        return "sum"
    elif "count" in check_name:
        return "count"
    elif "min" in check_name:
        return "min"
    elif "max" in check_name:
        return "max"
    elif "quantiles" in check_name:
        return "quantiles"
    return ""
