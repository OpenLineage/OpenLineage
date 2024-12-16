# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import json
import os
import subprocess
import tempfile
from functools import cached_property
from typing import Dict, Generator, List, Optional

from openlineage.client.event_v2 import RunEvent, RunState
from openlineage.client.facet_v2 import error_message_run, job_type_job, sql_job
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor
from openlineage.common.provider.dbt.processor import (
    DbtVersionRunFacet,
    ModelNode,
    ParentRunMetadata,
    UnsupportedDbtCommand,
)
from openlineage.common.provider.dbt.utils import (
    HANDLED_COMMANDS,
    generate_run_event,
    get_dbt_command,
    get_dbt_profiles_dir,
    get_event_timestamp,
    get_job_type,
    get_node_unique_id,
    get_parent_run_metadata,
)
from openlineage.common.utils import (
    add_command_line_arg,
    add_or_replace_command_line_option,
    get_from_nullable_chain,
    stream_has_lines,
)


class DbtStructuredLogsProcessor(DbtLocalArtifactProcessor):
    should_raise_on_unsupported_command = True

    def __init__(
        self,
        dbt_command_line: List[str],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.dbt_command_line = dbt_command_line
        self.profiles_dir = get_dbt_profiles_dir(command=self.dbt_command_line)

        self.node_id_to_ol_run_id: Dict[str, str] = {}

        # sql query ids are incremented sequentially per node_id
        self.node_id_to_sql_query_id: Dict[str, str] = {}
        self.node_id_to_sql_start_event: Dict[str, RunEvent] = {}

        self.node_id_to_inputs: Dict[str, List[ModelNode]] = {}
        self.node_id_to_output: Dict[str, ModelNode] = {}

        # will be populated when some dbt events are collected
        self._compiled_manifest: Dict = {}
        self._dbt_version: Optional[str] = None

    @cached_property
    def dbt_command(self):
        return get_dbt_command(self.dbt_command_line)

    @property
    def dbt_version(self):
        """
        Extracted from the first structured log MainReportVersion
        """
        return self._dbt_version

    @cached_property
    def catalog(self):
        if os.path.isfile(self.catalog_path):
            return self.load_metadata(self.catalog_path, [1], self.logger)
        return None

    @cached_property
    def profile(self):
        profile_dict = self.load_yaml_with_jinja(os.path.join(self.profiles_dir, "profiles.yml"))[
            self.profile_name
        ]
        if not self.target:
            self.target = profile_dict["target"]

        ze_profile = profile_dict["outputs"][self.target]
        return ze_profile

    @property
    def compiled_manifest(self) -> Dict:
        """
        Manifest is loaded and cached.
        It's loaded when the dbt structured logs are generated and processed.
        """
        if self._compiled_manifest != {}:
            return self._compiled_manifest
        elif os.path.isfile(self.manifest_path):
            self._compiled_manifest = self.load_metadata(self.manifest_path, [2, 3, 4, 5, 6, 7], self.logger)
            return self._compiled_manifest
        else:
            return {}

    def parse(self) -> Generator[RunEvent, None, None]:
        """
        This executes the dbt command and parses the structured log events emitted.
        OL events are sent when relevant.
        dbt structured events are generated example (NodeStart, NodeFinish, ...).
        """
        if self.dbt_command not in HANDLED_COMMANDS:
            raise UnsupportedDbtCommand(
                f"dbt integration for structured logs doesn't recognize dbt command "
                f"{self.dbt_command_line} - operation should be one of {HANDLED_COMMANDS}"
            )

        self.extract_adapter_type(self.profile)

        self.extract_dataset_namespace(self.profile)

        for line in self._run_dbt_command():
            self.logger.info(line)
            ol_event = self._parse_structured_log_event(line)
            if ol_event:
                yield ol_event

    def _parse_structured_log_event(self, line: str):
        """
        dbt generates structured events https://docs.getdbt.com/reference/events-logging
        relevant events are consumed to generate OL events.
        They are usually composed of start/finish events:
        1. NodeStart/NodeFinish for node lifecycle events
        2. MainReportVersion/CommandCompleted for dbt command lifecycle events
        3. SQLQuery/SQLQueryStatus/CatchableExceptionOnRun For SQL query lifecycle events
        """
        dbt_event = None
        try:
            dbt_event = json.loads(line)
        except ValueError:
            # log that can't be consumed
            return None

        dbt_event_name = dbt_event["info"]["name"]

        if dbt_event_name == "MainReportVersion":
            self._dbt_version = dbt_event["data"]["version"][1:]
            start_event = self._parse_dbt_start_command_event(dbt_event)
            self._setup_dbt_run_metadata(start_event)
            return start_event

        elif dbt_event_name == "CommandCompleted":
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

    def _parse_dbt_start_command_event(self, event):
        parent_run_metadata = get_parent_run_metadata()
        event_time = get_event_timestamp(event["info"]["ts"])
        run_facets = self.dbt_version_facet()
        if parent_run_metadata:
            run_facets["parent"] = parent_run_metadata

        start_event_run_id = str(generate_new_uuid())

        start_event = generate_run_event(
            event_type=RunState.START,
            event_time=event_time,
            run_id=start_event_run_id,
            job_name=self.job_name,
            job_namespace=self.job_namespace,
            run_facets=run_facets,
        )

        return start_event

    def _parse_node_start_event(self, event: Dict) -> RunEvent:
        node_unique_id = get_node_unique_id(event)
        if node_unique_id not in self.node_id_to_ol_run_id:
            self.node_id_to_ol_run_id[node_unique_id] = str(generate_new_uuid())

        run_id = self.node_id_to_ol_run_id[node_unique_id]
        node_start_time = get_event_timestamp(event["data"]["node_info"]["node_started_at"])

        parent_run_metadata = self.dbt_run_metadata.to_openlineage()
        run_facets = {"parent": parent_run_metadata, **self.dbt_version_facet()}

        job_name = self._get_job_name(event)
        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType=get_job_type(event),
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            )
        }

        inputs = [
            self.node_to_dataset(node=model_input, has_facets=True)
            for model_input in self._get_model_inputs(node_unique_id)
        ]
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
            outputs=outputs,
        )

    def _parse_node_finish_event(self, event):
        node_unique_id = get_node_unique_id(event)
        node_finished_at = get_event_timestamp(event["data"]["node_info"]["node_finished_at"])
        node_status = event["data"]["node_info"]["node_status"]
        run_id = self.node_id_to_ol_run_id[node_unique_id]

        parent_run_metadata = self.dbt_run_metadata.to_openlineage()
        run_facets = {"parent": parent_run_metadata, **self.dbt_version_facet()}

        job_name = self._get_job_name(event)

        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType=get_job_type(event),
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            )
        }

        event_type = RunState.COMPLETE

        if node_status != "success":
            event_type = RunState.FAIL
            error_message = event["data"]["run_result"]["message"]
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error_message, programmingLanguage="sql"
            )

        inputs = [
            self.node_to_dataset(node=model_input, has_facets=True)
            for model_input in self._get_model_inputs(node_unique_id)
        ]
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
            outputs=outputs,
        )

    def _parse_sql_query_event(self, event):
        node_unique_id = get_node_unique_id(event)
        node_start_run_id = self.node_id_to_ol_run_id[node_unique_id]
        sql_ol_run_id = str(generate_new_uuid())
        sql_start_at = event["info"]["ts"]

        parent_run = ParentRunMetadata(
            run_id=node_start_run_id, job_name=self._get_job_name(event), job_namespace=self.job_namespace
        )

        run_facets = {"parent": parent_run.to_openlineage(), **self.dbt_version_facet()}

        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType=get_job_type(event),
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            ),
            "sql": sql_job.SQLJobFacet(query=event["data"]["sql"]),
        }

        sql_event = generate_run_event(
            event_type=RunState.START,
            event_time=sql_start_at,
            run_id=sql_ol_run_id,
            run_facets=run_facets,
            job_name=self._get_sql_job_name(event),
            job_namespace=self.job_namespace,
            job_facets=job_facets,
        )

        self.node_id_to_sql_start_event[node_unique_id] = sql_event

        return sql_event

    def _parse_sql_query_status_event(self, event):
        """
        If the sql query is successful a SQLQueryStatus is generated by dbt.
        In case of failure a CatchableExceptionOnRun is generated instead
        """
        node_unique_id = get_node_unique_id(event)
        sql_ol_run_event = self.node_id_to_sql_start_event[node_unique_id]

        dbt_node_type = event["info"]["name"]
        run_state = RunState.OTHER
        event_time = get_event_timestamp(event["info"]["ts"])
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
            job_facets=sql_ol_run_event.job.facets,
        )

    def _parse_command_completed_event(self, event):
        success = event["data"]["success"]
        event_time = get_event_timestamp(event["data"]["completed_at"])
        run_facets = self.dbt_version_facet()
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

    def _setup_dbt_run_metadata(self, start_event):
        """
        The dbt command defines the parent run metadata of
        all subsequent OL events (NodeStart, NodeFinish ...)
        """
        self.dbt_run_metadata = ParentRunMetadata(
            run_id=start_event.run.runId,
            job_name=start_event.job.name,
            job_namespace=start_event.job.namespace,
        )

    def dbt_version_facet(self):
        return {"dbt_version": DbtVersionRunFacet(version=self.dbt_version)}

    def _get_sql_query_id(self, node_id):
        """
        Not all adapters have the sql id defined in their dbt event.
        A node is executed by a single thread which means that sql queries of a single node are executed
        sequentially and their status is also reported sequentially.
        This function gives us a surrogate query id. It's auto-incremented
        """
        if node_id not in self.node_id_to_sql_query_id:
            self.node_id_to_sql_query_id[node_id] = 1

        query_id = self.node_id_to_sql_query_id[node_id]
        self.node_id_to_sql_query_id[node_id] += 1
        return query_id

    def _get_sql_job_name(self, event):
        """
        The name of the sql job is as follows
        {node_job_name}.sql.{incremental_id}
        """
        node_job_name = self._get_job_name(event)
        node_unique_id = get_node_unique_id(event)
        query_id = self._get_sql_query_id(node_unique_id)
        job_name = f"{node_job_name}.sql.{query_id}"

        return job_name

    def _get_job_name(self, event):
        """
        The job name of models, snapshots ...
        """
        database = event["data"]["node_info"]["node_relation"]["database"]
        schema = event["data"]["node_info"]["node_relation"]["schema"]
        node_unique_id = get_node_unique_id(event)
        if node_unique_id.startswith("model"):
            node_id = self.removeprefix(node_unique_id, "model.")
            suffix = ".build.run" if self.dbt_command == "build" else ""
        elif node_unique_id.startswith("snapshot"):
            node_id = self.removeprefix(node_unique_id, "snapshot.")
            suffix = ".build.snapshot" if self.dbt_command == "build" else ".snapshot"
        else:
            node_id = node_unique_id
            suffix = ".build.run" if self.dbt_command == "build" else ""

        return f"{database}.{schema}.{node_id}{suffix}"

    def _run_dbt_command(self) -> Generator[str, None, None]:
        """
        This is a generator, it returns log lines
        """
        # log in json and generated the artifacts
        dbt_command_line = add_command_line_arg(
            self.dbt_command_line, arg_name="--log-format", arg_value="json"
        )
        dbt_command_line = add_or_replace_command_line_option(
            self.dbt_command_line, option="--write-json", replace_option="--no-write-json"
        )

        stdout_file = tempfile.NamedTemporaryFile(delete=False)
        stderr_file = tempfile.NamedTemporaryFile(delete=False)
        stdout_reader = open(stdout_file.name, mode="r")
        stderr_reader = open(stderr_file.name, mode="r")

        process = subprocess.Popen(dbt_command_line, stdout=stdout_file, stderr=stderr_file)

        while process.poll() is None:
            if stream_has_lines(stdout_reader):
                # Load the manifest as soon as it exists
                self.compiled_manifest

            yield from self._consume_dbt_logs(stdout_reader, stderr_reader)

        yield from self._consume_dbt_logs(stdout_reader, stderr_reader)

        stdout_reader.close()
        stderr_reader.close()

    def _consume_dbt_logs(self, stdout_reader, stderr_reader) -> Generator[str, None, None]:
        stdout_lines = stdout_reader.readlines()
        stderr_lines = stderr_reader.readlines()
        for line in stdout_lines:
            line = line.strip()
            if line:
                yield line

        for line in stderr_lines:
            line = line.strip()
            if line:
                self.logger.error(line)

    def _get_model_node(self, node_id) -> ModelNode:
        """
        Builds a ModelNode of a given node_id
        """
        all_nodes = {**self.compiled_manifest["nodes"], **self.compiled_manifest["sources"]}
        manifest_node = all_nodes[node_id]
        catalog_node = get_from_nullable_chain(self.catalog, ["nodes", node_id])
        return ModelNode(metadata_node=manifest_node, catalog_node=catalog_node)

    def _get_model_inputs(self, node_id) -> List[ModelNode]:
        """
        Builds the model's upstream inputs
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

    def get_dbt_metadata(
        self,
    ):
        """
        Replaced by properties
        """
        pass
