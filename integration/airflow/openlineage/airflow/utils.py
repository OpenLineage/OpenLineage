# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import datetime
import importlib
import json
import logging
import os
import subprocess
from typing import TYPE_CHECKING, Type, Dict, Any
from uuid import uuid4
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from typing import Optional
from airflow.models import DAG as AIRFLOW_DAG

from openlineage.airflow.facets import (
    AirflowMappedTaskRunFacet,
    AirflowVersionRunFacet,
    AirflowRunArgsRunFacet
)
from openlineage.client.utils import RedactMixin
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


def to_json_encodable(task: "BaseOperator") -> Dict[str, object]:
    def _task_encoder(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, AIRFLOW_DAG):
            return {'dag_id': obj.dag_id,
                    'tags': obj.tags,
                    'schedule_interval': obj.schedule_interval}
        else:
            return str(obj)

    return json.loads(json.dumps(task.__dict__, default=_task_encoder))


class SafeStrDict(dict):
    def __str__(self):
        castable = list()
        for key, val in self.items():
            try:
                str(key), str(val)
                castable.append((key, val))
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
    if parsed.query:
        query_dict = dict(parse_qsl(parsed.query))
        if conn.EXTRA_KEY in query_dict:
            query_dict = json.loads(query_dict[conn.EXTRA_KEY])
        filtered_qs = {
            k: v for k, v in query_dict.items() if not _filtered_query_params(k)
        }
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


def get_connection(conn_id) -> "Optional[Connection]":
    from airflow.hooks.base import BaseHook
    try:
        return BaseHook.get_connection(conn_id=conn_id)
    except Exception:
        return None


def get_job_name(task):
    return f'{task.dag_id}.{task.task_id}'


def get_custom_facets(
    dagrun, task, is_external_trigger: bool, task_instance: "TaskInstance" = None
) -> Dict[str, Any]:
    custom_facets = {
        "airflow_runArgs": AirflowRunArgsRunFacet(is_external_trigger),
        "airflow_version": AirflowVersionRunFacet.from_dagrun_and_task(dagrun, task),
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
        logging.info(e.msg)  # type: ignore
        return None


def redact_with_exclusions(source: Any):
    try:
        from airflow.utils.log.secrets_masker import (
            _secrets_masker,
            should_hide_value_for_key,
        )
    except ImportError:
        return source
    import copy

    sm = copy.deepcopy(_secrets_masker())
    MAX_RECURSION_DEPTH = 20

    def _redact(item, name: Optional[str], depth: int):
        if depth > MAX_RECURSION_DEPTH:
            return item
        try:
            if (
                name
                and should_hide_value_for_key(name)
            ):
                return sm._redact_all(item, depth)
            if isinstance(item, dict):
                return {
                    dict_key: _redact(subval, name=dict_key, depth=(depth + 1))
                    for dict_key, subval in item.items()
                }
            elif is_dataclass(item) or (is_json_serializable(item) and hasattr(item, '__dict__')):
                for dict_key, subval in item.__dict__.items():
                    if _is_name_redactable(dict_key, item):
                        setattr(
                            item,
                            dict_key,
                            _redact(subval, name=dict_key, depth=(depth + 1)),
                        )
                return item
            elif isinstance(item, str):
                if sm.replacer:
                    return sm.replacer.sub("***", item)
                return item
            elif isinstance(item, (tuple, set)):
                return tuple(
                    _redact(subval, name=None, depth=(depth + 1)) for subval in item
                )
            elif isinstance(item, list):
                return [
                    _redact(subval, name=None, depth=(depth + 1)) for subval in item
                ]
            else:
                return item
        except Exception as e:
            log.warning(
                "Unable to redact %s" "Error was: %s: %s",
                repr(item),
                type(e).__name__,
                str(e),
            )
            return item

    return _redact(source, name=None, depth=0)


def is_dataclass(item):
    return getattr(item.__class__, "__attrs_attrs__", None) is not None


def is_json_serializable(item):
    try:
        json.dumps(item)
        return True
    except (TypeError, ValueError):
        return False


def _is_name_redactable(name, redacted):
    if not issubclass(redacted.__class__, RedactMixin):
        return not name.startswith('_')
    return name not in redacted.skip_redact


class LoggingMixin:

    _log: Optional["logging.Logger"] = None

    @property
    def log(self) -> logging.Logger:
        """Returns a logger."""
        if self._log is None:
            self._log = logging.getLogger(self._get_logger_name())
        return self._log

    def _get_logger_name(self):
        if self.__class__.__module__.startswith("openlineage.airflow.extractors"):
            return self.__class__.__module__ + "." + self.__class__.__name__
        else:
            return (
                "openlineage.airflow.extractors."
                f"{self.__class__.__module__}.{self.__class__.__name__}"
            )
