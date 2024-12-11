# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os

import sys
import subprocess
from functools import cached_property
from typing import Any, Dict, List, Optional, Tuple

import tempfile

from openlineage.common.provider.dbt.processor import ModelNode
from openlineage.common.utils import get_from_nullable_chain


from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.common.utils import parse_single_arg
from openlineage.common.provider.dbt.processor import ParentRunMetadata
from openlineage.common.provider.dbt.utils import PRODUCER

from openlineage.client.facet_v2 import (
    job_type_job,
    sql_job,
    error_message_run
)



openlineage_logger = logging.getLogger("openlineage.dbt")
openlineage_logger.setLevel(os.getenv("OPENLINEAGE_DBT_LOGGING", "INFO"))
openlineage_logger.addHandler(logging.StreamHandler(sys.stdout))
# deprecated dbt-ol logger
logger = logging.getLogger("dbtol")
for handler in openlineage_logger.handlers:
    logger.addHandler(handler)
    logger.setLevel(openlineage_logger.level)


# list of commands that produce a run_result.json file
RUN_RESULT_COMMANDS = [
    "build", "compile", "docs generate", "run", "seed", "snapshot", "test", "run-operation"
]

# where we can use the ol wrapper
HANDLED_COMMANDS = [
    "run", "build", "test", "seed", "snapshot"
]

#todo not used
RELEVANT_EVENTS = [
    "MainReportVersion", "NodeStart", "SQLQuery", "SQLQueryStatus", "NodeFinished", "CatchableExceptionOnRun"
]

class DbtStructuredLoggingProcessor(DbtLocalArtifactProcessor):
    should_raise_on_unsupported_command = True

    def __init__(
            self,
            dbt_command: List[str],
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.dbt_command = dbt_command
        self.profiles_dir = get_dbt_profiles_dir(command=self.dbt_command)

        self.node_id_to_ol_run_id = {}

        # sql query ids are incremented sequentially per node_id
        self.node_id_to_sql_query_id = {}
        self.node_id_to_sql_start_event = {}

        self.node_id_to_inputs = {}
        self.node_id_to_output = {}

        # will be populated as soon as the manifest is generated
        self._compiled_manifest = None


    @cached_property
    def dbt_command(self):
        return get_dbt_command(self.dbt_command)


    def parse(self) -> List[RunEvent]:
        """
        todo update
        """

        if self.dbt_command not in HANDLED_COMMANDS:
            # do nothing
            # todo add logs 
            ...

        self.extract_adapter_type(self.profile)

        self.extract_dataset_namespace(self.profile)

        for line in self._run_dbt_command():
            logger.info(line)
            ol_event = self._parse_structured_log_event(line)
            if ol_event:
                yield ol_event


    def _parse_dbt_start_command_event(self, event):
        parent_run_metadata = get_parent_run_metadata()
        event_time = event["info"]["ts"]
        run_facets = {}
        if parent_run_metadata:
            run_facets = {"parent": parent_run_metadata}

        start_event_run_id = str(generate_new_uuid())

        start_event = generate_run_event(
            event_type=RunState.START,
            event_time=event_time,
            run_id=start_event_run_id,
            job_name=self.job_name,
            job_namespace=self.job_namespace,
            run_facets=run_facets
        )

        return start_event

    def _setup_dbt_run_metadata(self, start_event):
        """
        the dbt command defines the parent run metadata of all subsequent ol events (node start, node finish ...)
        """
        self.dbt_run_metadata = ParentRunMetadata(
            run_id=start_event.run.runId,
            job_name=start_event.job.name,
            job_namespace=start_event.job.namespace,
        )


    #todo is this used ??? legacy from the parent class. Try to remove this
    @cached_property
    def dbt_version(self):
        """
        1. rely on dbt --version
        2. rely on compile manifest
        3. rely on first log
        """
        return self.compiled_manifest["metadata"]["dbt_version"]

    def _parse_structured_log_event(self, line: str):
        """
        #todo rephrase and
        let's start with the dbt run scenario

        There is only one NodeStart event from dbt
        for a NodeStart
        we have a map from unique_id -> run_uuid

        For a single unique_id node
        1. create a facet for the parent dbt command
        2. for event NodeStart create a Start OL
           a. add the input and output from the manifest
        3. for SQLQuery create a Start OL event with the NodeStart parent run _id
        4. for SQLQueryStatus create either running/completed/failed event

        SQLQueryStatus refer the last query executed.
        lists of events
        "NodeStart", "SQLQuery", "SQLQueryStatus", "NodeFinished", "CatchableExceptionOnRun", "CommandCompleted"
        """

        dbt_event = None
        try:
            dbt_event = json.loads(line)
        except ValueError:
            # log that we can't be consumed
            return None

        dbt_event_name = dbt_event["info"]["name"]

        # only happens once
        if dbt_event_name == "MainReportVersion":
            # dbt command is started
            start_event = self._parse_dbt_start_command_event(dbt_event)
            self._setup_dbt_run_metadata(start_event)
            return start_event

        elif dbt_event_name == "CommandCompleted":
            # dbt command finishes
            end_event = self._parse_command_completed_event(dbt_event)
            return end_event

        try:
            get_node_unique_id(dbt_event)
        except KeyError:
            return None

        ol_event = None

        if dbt_event_name == "NodeStart":
            ol_event = self._parse_node_start_event(dbt_event)
        elif dbt_event_name == "SQLQuery":
            ol_event = self._parse_sql_query_event(dbt_event)
        elif dbt_event_name in ("SQLQueryStatus", "CatchableExceptionOnRun"):
            ol_event = self._parse_sql_query_status_event(dbt_event)
        elif dbt_event_name == "NodeFinished":
            ol_event = self._parse_node_finish_event(dbt_event)

        return ol_event


    def _parse_node_start_event(self, event: Dict) -> RunEvent:
        node_unique_id = get_node_unique_id(event)
        if node_unique_id not in self.node_id_to_ol_run_id:
            self.node_id_to_ol_run_id[node_unique_id] = str(generate_new_uuid())

        run_id = self.node_id_to_ol_run_id[node_unique_id]
        node_start_time = event["data"]["node_info"]["node_started_at"]

        parent_run_metadata = self.dbt_run_metadata.to_openlineage()
        run_facets = {"parent": parent_run_metadata}


        job_name = self._get_job_name(event)
        job_facets = { "jobType": job_type_job.JobTypeJobFacet(
            jobType=get_job_type(event),
            integration="DBT",
            processingType="BATCH",
            producer=self.producer,
        )}

        inputs = [self.node_to_dataset(node=model_input, has_facets=True) for model_input in self._get_model_inputs(node_unique_id)]
        outputs = [self.node_to_output_dataset(node=self._get_model_node(node_unique_id), has_facets=True)]

        return generate_run_event(
            event_type=RunState.START,
            event_time=node_start_time,
            run_id=run_id,
            run_facets=run_facets,
            job_name=job_name,
            job_namespace=self.job_namespace,
            job_facets=job_facets,
            inputs=inputs,
            outputs=outputs
        )

    def _parse_node_finish_event(self, event):
        node_unique_id = get_node_unique_id(event)
        node_finished_at = event["data"]["node_info"]["node_finished_at"]
        node_status = event["data"]["node_info"]["node_status"]
        run_id = self.node_id_to_ol_run_id[node_unique_id]

        parent_run_metadata = self.dbt_run_metadata.to_openlineage()
        run_facets = {"parent": parent_run_metadata}

        job_name = self._get_job_name(event)

        job_facets = { "jobType": job_type_job.JobTypeJobFacet(
            jobType=get_job_type(event),
            integration="DBT",
            processingType="BATCH",
            producer=self.producer,
        )}

        event_type = RunState.COMPLETE

        if node_status != "success":
            event_type = RunState.FAIL
            error_message = event["data"]["run_result"]["message"]
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error_message, programmingLanguage="sql"
            )

        inputs = [self.node_to_dataset(node=model_input, has_facets=True) for model_input in self._get_model_inputs(node_unique_id)]
        outputs = [self.node_to_output_dataset(node=self._get_model_node(node_unique_id), has_facets=True)]

        return generate_run_event(
            event_type=event_type,
            event_time=node_finished_at,
            run_id=run_id,
            run_facets=run_facets,
            job_name=job_name,
            job_namespace=self.job_namespace,
            job_facets=job_facets,
            inputs=inputs,
            outputs=outputs
        )


    def _parse_sql_query_event(self, event):
        node_unique_id = get_node_unique_id(event)
        node_start_run_id = self.node_id_to_ol_run_id[node_unique_id]
        sql_ol_run_id = str(generate_new_uuid())
        sql_start_at = event["info"]["ts"]


        parent_run = ParentRunMetadata(
            run_id=node_start_run_id,
            job_name= self._get_job_name(event),
            job_namespace=self.job_namespace
        )

        run_facets = {"parent": parent_run.to_openlineage()}

        job_facets = { "jobType": job_type_job.JobTypeJobFacet(
            jobType=get_job_type(event),
            integration="DBT",
            processingType="BATCH",
            producer=self.producer,
        ),
            "sql": sql_job.SQLJobFacet(query=event["data"]["sql"])
        }

        sql_event =  generate_run_event(
            event_type=RunState.START,
            event_time=sql_start_at,
            run_id=sql_ol_run_id,
            run_facets=run_facets,
            job_name=self._get_sql_job_name(event),
            job_namespace=self.job_namespace,
            job_facets=job_facets
        )

        self.node_id_to_sql_start_event[node_unique_id] = sql_event

        return sql_event


    def _get_sql_query_id(self, node_id):
        """
        not all adapters have the sql id defined in the dbt event.
        A node is executed by a single thread which means that sql queries of a single node are executed
        sequentially and their status is also reported sequentially
        this function gives us a surrogate query id. it's auto-incremented
        """
        query_id = None
        if node_id not in self.node_id_to_sql_query_id:
            self.node_id_to_sql_query_id[node_id] = 1

        query_id = self.node_id_to_sql_query_id[node_id]
        self.node_id_to_sql_query_id[node_id] += 1
        return query_id


    def _parse_sql_query_status_event(self, event):
        """
        if the sql query is successful a SQLQueryStatus is generated by dbt
        in case of failure a CatchableExceptionOnRun is generated instead
        """
        node_unique_id = get_node_unique_id(event)
        sql_ol_run_event = self.node_id_to_sql_start_event[node_unique_id]

        dbt_node_type = event["info"]["name"]
        run_state = RunState.OTHER
        event_time = event["info"]["ts"]
        run_facets = sql_ol_run_event.run.facets

        if dbt_node_type == "CatchableExceptionOnRun":
            run_state = RunState.FAIL
            error_message = event["data"]["exc"]
            stacktrace = event["data"]["exc_info"]
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                    message=error_message, programmingLanguage="python", stackTrace=stacktrace
                )
        elif dbt_node_type == "SQLQueryStatus":
            run_state = RunState.COMPLETE


        return generate_run_event(
            event_type=run_state,
            event_time=event_time,
            run_id=sql_ol_run_event.run.runId,
            run_facets=run_facets,
            job_name=sql_ol_run_event.job.name,
            job_namespace=sql_ol_run_event.job.namespace,
            job_facets=sql_ol_run_event.job.facets
        )

    def _parse_command_completed_event(self, event):
        success = event["data"]["success"]
        event_time = event["data"]["completed_at"]
        run_facets = {}
        run_state = None
        if success:
            run_state = RunState.COMPLETE
        else:
            run_state = RunState.FAIL
            error_message = event["info"]["msg"]
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error_message, programmingLanguage="python"
            )

        parent_run_metadata = get_parent_run_metadata()

        if parent_run_metadata:
            run_facets["parent"] = parent_run_metadata

        return generate_run_event(
            event_type=run_state,
            event_time=event_time,
            run_id=self.dbt_run_metadata.run_id,
            job_name=self.dbt_run_metadata.job_name,
            job_namespace=self.dbt_run_metadata.job_namespace,
            run_facets=run_facets,
        )

    def _get_sql_job_name(self, event):
        """
        the name of the sql job is as follows
        {node_job_name}.sql.{incremental_id}
        """
        node_job_name = self._get_job_name(event)
        node_unique_id = get_node_unique_id(event)
        query_id = self._get_sql_query_id(node_unique_id)
        job_name = f"{node_job_name}.sql.{query_id}"
        
        return job_name

    def _get_job_name(self, event):
        database = event["data"]["node_info"]["node_relation"]["database"]
        schema = event["data"]["node_info"]["node_relation"]["schema"]
        node_unique_id = get_node_unique_id(event)
        node_id = self.removeprefix(node_unique_id, "model.") # todo remove prefix for "snapshot." as well
        suffix = ".build.run" if self.dbt_command == "build" else "" #todo why do we have this ?

        return f"{database}.{schema}.{node_id}{suffix}"

    def _get_job_type(self, event) -> Optional[str]:
        node_unique_id = get_node_unique_id(event)
        node_type = event["info"]["name"]
        if node_type == "SQLQuery":
            return "SQL"
        elif node_unique_id.startswith("model."):
            return "MODEL"
        elif node_unique_id.startswith("snapshot."):
            return "SNAPSHOT"
        return None


    def _run_dbt_command(self):
        """
        this is a generator, it returns log lines
        """
        # log in json and generated the artifacts

        dbt_command = self.dbt_command + ["--log-format", "json", "--write-json"]
        stdout_file = tempfile.NamedTemporaryFile(delete=False)
        stderr_file = tempfile.NamedTemporaryFile(delete=False)
        stdout_reader = open(stdout_file.name, mode="r")
        stderr_reader = open(stderr_file.name, mode="r")

        process = subprocess.Popen(dbt_command, stdout=stdout_file, stderr=stderr_file)

        while process.poll() is None:
            stdout_lines = stdout_reader.readlines()
            stderr_lines = stderr_reader.readlines()

            if len(stdout_lines) > 0:
                self._load_manifest()

            for line in stdout_lines:
                line = line.strip()
                if line:
                    yield line

            for line in stderr_lines:
                line = line.strip()
                if line:
                    logger.error(line)

        stdout_reader.close()
        stderr_reader.close()


    def _get_model_node(self, node_id) -> ModelNode:
        """
        builds a ModelNode of a given node_id
        """
        all_nodes = {**self.compiled_manifest["nodes"], **self.compiled_manifest["sources"]}
        manifest_node = all_nodes[node_id]
        catalog_node = get_from_nullable_chain(self.catalog, ["nodes", node_id])
        return ModelNode(
            metadata_node=manifest_node,
            catalog_node=catalog_node
        )

    def _get_model_inputs(self, node_id) -> List[ModelNode]:
        """
        build the model's upstream inputs
        """
        if node_id in self.node_id_to_inputs:
            return self.node_id_to_inputs[node_id]

        input_node_ids = self.compiled_manifest["parent_map"][node_id]
        inputs = [self._get_model_node(input_node_id) for input_node_id in input_node_ids]
        self.node_id_to_inputs[node_id] = inputs

        return inputs


    def _get_model_output(self, node_id) -> ModelNode:
        if node_id in self.node_id_to_output:
            return self.node_id_to_output[node_id]
        model_node = self._get_model_node(node_id)
        self.node_id_to_output[node_id] = model_node

        return model_node


    @cached_property
    def catalog(self):
        if os.path.isfile(self.catalog_path):
            return self.load_metadata(self.catalog_path, [1], self.logger)
        return None

    @cached_property
    def profile(self):
        profile_dict = self.load_yaml_with_jinja(os.path.join(self.profiles_dir, "profiles.yml"))[self.profile_name]
        if not self.target:
            self.target = profile_dict["target"]

        ze_profile = profile_dict["outputs"][self.target]
        return ze_profile


    def _load_manifest(self):
        """
        load the manifest as soon as it exists
        """
        self.compiled_manifest


    @property
    def compiled_manifest(self) -> Optional[Dict]:
        """
        manifest is loaded and cached
        """
        if self._compiled_manifest is not None:
            return self._compiled_manifest
        elif os.path.isfile(self.manifest_path):
            self._compiled_manifest = self.load_metadata(self.manifest_path, [2, 3, 4, 5, 6, 7], self.logger)
            return self._compiled_manifest
        else:
            return None


    def get_dbt_metadata(
            self,
    ) -> Tuple[Dict[Any, Any], Optional[Dict[Any, Any]], Dict[Any, Any], Optional[Dict[Any, Any]]]:
        """
        replaced by cached properties
        """
        pass



############
# helpers
############

def get_dbt_command(dbt_command: List[str]) -> Optional[str]:
    dbt_command_tokens = set(dbt_command)
    for command in HANDLED_COMMANDS:
        if command in dbt_command_tokens:
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
        producer=PRODUCER
    )

def get_dbt_profiles_dir(command: List[str]) -> str:
    """
    based on this https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles#advanced-customizing-a-profile-directory
    gets the profiles directory
    """
    from_command = parse_single_arg(command, ["--profiles-dir"])
    from_env_var = os.getenv("DBT_PROFILES_DIR")
    default_dir = "~/.dbt/"
    return (
            from_command or
            from_env_var or
            default_dir
    )

def get_parent_run_metadata():
    """
    the parent job that started the dbt command. Usually the scheduler (Airflow, ...etc)
    """
    parent_id = os.getenv("OPENLINEAGE_PARENT_ID")
    parent_run_metadata = None
    if parent_id:
        parent_namespace, parent_job_name, parent_run_id = parent_id.split("/")
        parent_run_metadata = ParentRunMetadata(
            run_id=parent_run_id,
            job_name=parent_job_name,
            job_namespace=parent_namespace)
    return parent_run_metadata


def get_node_unique_id(event):
    return event["data"]["node_info"]["unique_id"]


def get_job_type(event) -> Optional[str]:
    """
    gets the Run Event's job type
    """
    node_unique_id = get_node_unique_id(event)
    node_type = event["info"]["name"]
    if node_type == "SQLQuery":
        return "SQL"
    elif node_unique_id.startswith("model."):
        return "MODEL"
    elif node_unique_id.startswith("snapshot."):
        return "SNAPSHOT"
    return None
