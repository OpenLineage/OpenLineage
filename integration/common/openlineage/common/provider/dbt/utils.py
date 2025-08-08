# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import logging
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.common.provider.dbt.facets import ParentRunMetadata
from openlineage.common.utils import get_from_nullable_chain, parse_single_arg

__version__ = "1.37.0"
PRODUCER = f"https://github.com/OpenLineage/OpenLineage/tree/{__version__}/integration/dbt"

# for which command structured logs consumption is implemented
HANDLED_COMMANDS = ["run", "seed", "snapshot", "test", "build"]
CONSUME_STRUCTURED_LOGS_COMMAND_OPTION = "--consume-structured-logs"
OPENLINEAGE_DBT_JOB_NAME_OPTION = "--openlineage-dbt-job-name"
DBT_LOG_FILE_MAX_BYTES = str(5 * 1024 * 1024 * 1024)

log = logging.getLogger(__name__)


def get_event_timestamp(timestamp: str):
    """
    dbt events have a discrepancy in their timestamp formats
    This converts a given timestamp string to %Y-%m-%dT%H:%M:%S.%fZ
    It returns the given timestamp if it couldn't do the conversion
    """
    output_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    input_formats = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f"]
    for input_format in input_formats:
        try:
            iso_timestamp = datetime.strptime(timestamp, input_format).strftime(output_format)
            return iso_timestamp
        except ValueError:
            pass  # ignore and pass to the other format

    return timestamp


def get_dbt_command(dbt_command_line: List[str]) -> Optional[str]:
    dbt_command_line_tokens = set(dbt_command_line)
    for command in HANDLED_COMMANDS:
        if command in dbt_command_line_tokens:
            return command
    return None


def generate_run_event(
    event_type: RunState,
    event_time: str,
    run_id: str,
    job_name: str,
    job_namespace: str,
    inputs: Optional[List[InputDataset]] = None,
    outputs: Optional[List[OutputDataset]] = None,
    job_facets: Optional[Dict] = None,
    run_facets: Optional[Dict] = None,
) -> RunEvent:
    inputs = inputs or []
    outputs = outputs or []
    job_facets = job_facets or {}
    run_facets = run_facets or {}
    return RunEvent(
        eventType=event_type,
        eventTime=event_time,
        run=Run(runId=run_id, facets=run_facets),
        job=Job(
            namespace=job_namespace,
            name=job_name,
            facets=job_facets,
        ),
        inputs=inputs,
        outputs=outputs,
        producer=PRODUCER,
    )


def get_dbt_profiles_dir(command: List[str]) -> str:
    """
    Based on https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles#advanced-customizing-a-profile-directory
    Gets the profiles directory
    """
    from_command = parse_single_arg(command, ["--profiles-dir"])
    from_env_var = os.getenv("DBT_PROFILES_DIR")
    default_dir = "~/.dbt/"
    current_working_directory = os.getcwd()
    return from_command or from_env_var or current_working_directory or default_dir


def get_dbt_log_path(command: List[str]) -> str:
    """
    Based on this https://docs.getdbt.com/reference/global-configs/logs
    Gets the absolute path of the dbt log file.
    If the user doesn't specify the log path, we generate a random name for the logs directory
    """
    project_dir: str = parse_single_arg(command, ["--project-dir"], default="./")
    default_log_dirname = os.path.expanduser(os.path.join(project_dir, generate_random_log_file_name()))
    from_command = parse_single_arg(command, ["--log-path"], default=None)
    from_env_var = os.getenv("DBT_LOG_PATH")
    log_dirname = from_command or from_env_var or default_log_dirname
    return os.path.join(log_dirname, "dbt.log")


def is_random_logfile(command: List[str]) -> bool:
    from_command = parse_single_arg(command, ["--log-path"], default=None)
    from_env_var = os.getenv("DBT_LOG_PATH")
    if from_env_var or from_command:
        return False
    return True


def generate_random_log_file_name() -> str:
    random_uuid = str(uuid.uuid4())
    log_directory_name = f"dbt-logs-{random_uuid}"
    return log_directory_name


def get_parent_run_metadata():
    """
    The parent job that started the dbt command. Usually the scheduler (Airflow, ...etc)
    """
    parent_id = os.getenv("OPENLINEAGE_PARENT_ID")
    root_parent_id = os.getenv("OPENLINEAGE_ROOT_PARENT_ID")
    parent_run_metadata = None
    if parent_id:
        parent_tuple = parent_id.split("/")
        if len(parent_tuple) == 3:
            parent_namespace, parent_job_name, parent_run_id = parent_tuple
        else:
            log.warning("Received OPENLINEAGE_PARENT_ID but can't parse it")
            return None

        if root_parent_id:
            root_parent_tuple = root_parent_id.split("/")
            if len(root_parent_tuple) == 3:
                root_parent_job_namespace, root_parent_job_name, root_parent_run_id = root_parent_tuple
            else:
                root_parent_job_namespace, root_parent_job_name, root_parent_run_id = None, None, None
                log.warning("Received OPENLINEAGE_ROOT_PARENT_ID but can't parse it")
        else:
            root_parent_run_id = parent_run_id
            root_parent_job_name = parent_job_name
            root_parent_job_namespace = parent_namespace

        parent_run_metadata = ParentRunMetadata(
            run_id=parent_run_id,
            job_name=parent_job_name,
            job_namespace=parent_namespace,
            root_parent_job_name=root_parent_job_name,
            root_parent_job_namespace=root_parent_job_namespace,
            root_parent_run_id=root_parent_run_id,
        )
    return parent_run_metadata


def get_node_unique_id(event):
    return get_from_nullable_chain(event, ["data", "node_info", "unique_id"])


def get_job_type(event) -> Optional[str]:
    """
    Gets the Run Event's job type
    """
    node_unique_id = get_node_unique_id(event)
    node_type = event["info"]["name"]
    if node_type == "SQLQuery":
        return "SQL"
    elif node_type in ("MainReportVersion", "CommandCompleted"):
        return "JOB"
    elif node_unique_id.startswith("model."):
        return "MODEL"
    elif node_unique_id.startswith("snapshot."):
        return "SNAPSHOT"
    elif node_unique_id.startswith("seed."):
        return "SEED"
    elif node_unique_id.startswith("test."):
        return "TEST"

    return None
