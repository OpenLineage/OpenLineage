# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime
import json
import os
import subprocess
import sys
import time
from collections import defaultdict
from functools import cached_property, wraps
from typing import Dict, Generator, List, NamedTuple, Optional, TextIO

from openlineage.client.event_v2 import Dataset, RunEvent, RunState
from openlineage.client.facet_v2 import (
    data_quality_assertions_dataset as dq,
)
from openlineage.client.facet_v2 import (
    error_message_run,
    external_query_run,
    job_type_job,
    processing_engine_run,
    sql_job,
    tags_run,
)
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.facets import DbtRunRunFacet, DbtVersionRunFacet, ParentRunMetadata
from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor
from openlineage.common.provider.dbt.processor import (
    ModelNode,
    UnsupportedDbtCommand,
)
from openlineage.common.provider.dbt.utils import (
    DBT_LOG_FILE_MAX_BYTES,
    HANDLED_COMMANDS,
    generate_run_event,
    get_dbt_command,
    get_dbt_log_path,
    get_dbt_profiles_dir,
    get_event_timestamp,
    get_job_type,
    get_node_unique_id,
    get_parent_run_metadata,
    is_random_logfile,
)
from openlineage.common.provider.dbt.utils import (
    __version__ as openlineage_version,
)
from openlineage.common.utils import (
    IncrementalFileReader,
    add_command_line_args,
    add_or_replace_command_line_option,
    get_from_nullable_chain,
    has_lines,
)


class ManifestIntegrityResult(NamedTuple):
    """Result of manifest integrity validation.

    Attributes:
        is_valid: bool indicating if manifest is valid
        missing_nodes: list of node IDs that are missing
        missing_parents: list of parent node IDs that are missing
        missing_children: list of child node IDs that are missing
        total_missing: total count of missing nodes
        parent_map_size: total number of entries in parent_map
        available_nodes: total number of available nodes (nodes + sources)
    """

    is_valid: bool
    missing_nodes: List[str]
    missing_parents: List[str]
    missing_children: List[str]
    total_missing: int
    parent_map_size: int
    available_nodes: int


def handle_keyerror(func):
    """
    Handle KeyError exceptions when node_id was not found in the dbt manifest.
    """

    @wraps(func)
    def wrapper(self, node_id: str, *args, **kwargs):
        try:
            return func(self, node_id, *args, **kwargs)
        except KeyError:
            self.logger.warning(
                "Did not find node_id %s in the dbt manifest - did "
                "the manifest change while the job was executing?",
                node_id,
            )
            return None

    return wrapper


class DbtStructuredLogsProcessor(DbtLocalArtifactProcessor):
    should_raise_on_unsupported_command = True

    def __init__(
        self,
        dbt_command_line: List[str],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.dbt_command_line: List[str] = dbt_command_line
        self.profiles_dir: str = get_dbt_profiles_dir(command=self.dbt_command_line)
        self.dbt_log_file_path: str = get_dbt_log_path(command=self.dbt_command_line)
        self.is_random_logfile: bool = is_random_logfile(command=self.dbt_command_line)
        self.dbt_log_dirname: str = os.path.dirname(self.dbt_log_file_path)
        self.parent_run_metadata: ParentRunMetadata = get_parent_run_metadata()

        self.node_id_to_ol_run_id: Dict[str, str] = defaultdict(lambda: str(generate_new_uuid()))

        # sql query ids are incremented sequentially per node_id
        self.node_id_to_sql_query_id: Dict[str, Dict[str, int]] = defaultdict(lambda: {"next_id": 1})
        self.node_id_to_sql_start_event: Dict[str, RunEvent] = {}

        self.node_id_to_inputs: Dict[str, List[ModelNode]] = {}
        self.node_id_to_output: Dict[str, ModelNode] = {}

        # will be populated when some dbt events are collected
        self._compiled_manifest: Dict = {}
        self._dbt_version: Optional[str] = None
        self._dbt_invocation_id: Optional[str] = None
        self._dbt_log_file: Optional[TextIO] = None
        self.received_dbt_command_completed = False
        self.processed_bytes = 0

        self.dbt_command_return_code = 0

    @cached_property
    def dbt_command(self) -> Optional[str]:
        return get_dbt_command(self.dbt_command_line)

    @property
    def dbt_version(self) -> Optional[str]:
        """
        Extracted from the first structured log MainReportVersion
        """
        return self._dbt_version

    @property
    def invocation_id(self) -> Optional[str]:
        return self._dbt_invocation_id

    @cached_property
    def catalog(self) -> Optional[Dict]:
        if os.path.isfile(self.catalog_path):
            return self.load_metadata(self.catalog_path, [1], self.logger)
        return None

    @cached_property
    def profile(self):
        profile_dict = self.load_yaml_with_jinja(
            os.path.expanduser(os.path.join(self.profiles_dir, "profiles.yml"))
        )[self.profile_name]
        if not self.target:
            self.target = profile_dict["target"]

        current_profile = profile_dict["outputs"][self.target]
        return current_profile

    @property
    def compiled_manifest(self) -> Dict:
        """
        Manifest is loaded and cached.
        It's loaded when the dbt structured logs are generated and processed.
        """
        if self._compiled_manifest != {}:
            return self._compiled_manifest
        elif os.path.isfile(self.manifest_path):
            self._compiled_manifest = self.load_metadata(self.manifest_path, list(range(2, 13)), self.logger)
            manifest_data = self._validate_manifest_integrity()
            self.logger.debug(manifest_data)
            return self._compiled_manifest
        else:
            return {}

    def parse(self) -> Generator[RunEvent, None, None]:  # type: ignore[override]
        """
        This executes the dbt command and parses the structured log events emitted.
        OL events are generated and yielded when relevant.
        """
        if self.dbt_command not in HANDLED_COMMANDS:
            raise UnsupportedDbtCommand(
                f"dbt integration for structured logs doesn't recognize dbt command "
                f"{self.dbt_command_line} - operation should be one of {HANDLED_COMMANDS}"
            )

        self.extract_adapter_type(self.profile)

        self.extract_dataset_namespace(self.profile)

        for line in self._run_dbt_command():
            ol_event = self._parse_structured_log_event(line)
            if ol_event:
                yield ol_event

        if not self.received_dbt_command_completed:
            # We did not receive the CommandCompleted event, so we emit an abort event
            self.logger.debug("CommandCompleted event was not received. ABORT event will be send.")
            ol_event = self._get_dbt_command_abort_event()
            if ol_event:
                yield ol_event

    def _parse_structured_log_event(self, line: str) -> Optional[RunEvent]:
        """
        dbt generates structured events https://docs.getdbt.com/reference/events-logging
        relevant events are consumed to generate OL events.
        They are usually composed of start/finish events:
        1. NodeStart/NodeFinish for node lifecycle events
        2. MainReportVersion/CommandCompleted for dbt command lifecycle events
        3. SQLQuery/SQLQueryStatus/CatchableExceptionOnRun For SQL query lifecycle events
        """
        self.processed_bytes += len(line)
        self.logger.debug(
            "Received event of size %d. Total processed bytes %d", len(line), self.processed_bytes
        )
        try:
            dbt_event = json.loads(line)
        except ValueError:
            # log that can't be consumed
            self.logger.error("The following log is not valid JSON:\n%s", line)
            return None

        dbt_event_name = dbt_event["info"]["name"]
        self.logger.debug("Parsing dbt event: %s", dbt_event_name)
        if dbt_event_name == "MainReportVersion":
            self._dbt_version = dbt_event["data"]["version"][1:]
            self._dbt_invocation_id = dbt_event["info"].get("invocation_id")
            start_event = self._parse_dbt_start_command_event(dbt_event)
            self._setup_dbt_run_metadata(start_event)
            return start_event

        elif dbt_event_name == "CommandCompleted":
            end_event = self._parse_command_completed_event(dbt_event)
            self.received_dbt_command_completed = True
            return end_event

        if get_node_unique_id(dbt_event) is None:
            return None

        ol_event = None

        if dbt_event_name == "NodeStart":
            ol_event = self._parse_node_start_event(dbt_event)
        elif dbt_event_name == "SQLQuery":
            ol_event = self._parse_sql_query_event(dbt_event)
        elif dbt_event_name in ("SQLQueryStatus", "CatchableExceptionOnRun"):
            ol_event = self._parse_sql_query_status_event(dbt_event)
        elif dbt_event_name == "NodeFinished":
            ol_event = self.parse_node_finished_event(dbt_event)

        return ol_event

    def _parse_dbt_start_command_event(self, event) -> RunEvent:
        event_time = get_event_timestamp(event["info"]["ts"])
        run_facets = {
            **self.dbt_version_facet(),
            **self.processing_engine_facet(),
            **self.dbt_run_run_facet(),
        }

        if self.parent_run_metadata:
            run_facets["parent"] = self.parent_run_metadata.to_openlineage()

        start_event_run_id = str(generate_new_uuid())
        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType=get_job_type(event),
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            )
        }

        start_event = generate_run_event(
            event_type=RunState.START,
            event_time=event_time,
            run_id=start_event_run_id,
            job_name=self.job_name,
            job_namespace=self.job_namespace,
            job_facets=job_facets,
            run_facets=run_facets,
        )

        return start_event

    def _get_dbt_command_abort_event(self):
        run_facets = {
            **self.dbt_version_facet(),
            **self.processing_engine_facet(),
            **self.dbt_run_run_facet(),
        }

        parent_run_metadata = get_parent_run_metadata()
        if parent_run_metadata:
            run_facets["parent"] = parent_run_metadata.to_openlineage()

        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="JOB",
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            )
        }
        if not self.dbt_run_metadata:
            return None
        return generate_run_event(
            event_type=RunState.ABORT,
            event_time=datetime.datetime.now().isoformat(),  # Current time - no other data source
            run_id=self.dbt_run_metadata.run_id,
            job_name=self.dbt_run_metadata.job_name,
            job_namespace=self.dbt_run_metadata.job_namespace,
            job_facets=job_facets,
            run_facets=run_facets,
        )

    def _parse_node_start_event(self, event: Dict) -> RunEvent:
        node_unique_id = get_node_unique_id(event)
        run_id = self.node_id_to_ol_run_id[node_unique_id]
        node_start_time = get_event_timestamp(event["data"]["node_info"]["node_started_at"])

        run_facets = {
            **self.dbt_version_facet(),
            **self.processing_engine_facet(),
            **self.dbt_run_run_facet(),
            "parent": self.dbt_run_metadata.to_openlineage(),
        }

        # Add tags if they exist for this node
        if tags := self._get_node_tags(node_unique_id):
            run_facets["tags"] = tags_run.TagsRunFacet(
                tags=[tags_run.TagsRunFacetFields(key=tag, value="true", source="DBT") for tag in tags]
            )

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
        outputs = []
        if node := self._get_model_node(node_unique_id):
            outputs = [self.node_to_output_dataset(node=node, has_facets=True)]

        return generate_run_event(
            event_type=RunState.START,
            event_time=node_start_time,
            run_id=run_id,
            run_facets=run_facets,
            job_name=job_name,
            job_namespace=self.job_namespace,
            job_facets=job_facets,
            inputs=inputs,  # type: ignore[arg-type]
            outputs=outputs,
        )

    def parse_node_finished_event(self, event) -> RunEvent:
        resource_type = event["data"]["node_info"]["resource_type"]
        node_unique_id = get_node_unique_id(event)
        node_finished_at = get_event_timestamp(event["data"]["node_info"]["node_finished_at"])
        if node_finished_at == "":
            node_finished_at = get_event_timestamp(event["data"]["node_info"]["node_started_at"])
        node_status = event["data"]["node_info"]["node_status"]
        run_id = self.node_id_to_ol_run_id[node_unique_id]

        run_facets = {
            **self.dbt_version_facet(),
            **self.processing_engine_facet(),
            **self.dbt_run_run_facet(),
            "parent": self.dbt_run_metadata.to_openlineage(),
        }

        # Add tags if they exist for this node
        if tags := self._get_node_tags(node_unique_id):
            run_facets["tags"] = tags_run.TagsRunFacet(
                tags=[tags_run.TagsRunFacetFields(key=tag, value="true", source="DBT") for tag in tags]
            )

        job_name = self._get_job_name(event)
        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType=get_job_type(event),
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            )
        }

        event_type = RunState.OTHER

        if node_status == "skipped":
            event_type = RunState.ABORT
        elif node_status in ("fail", "error", "runtime error"):
            event_type = RunState.FAIL
            error_message = event["data"]["run_result"]["message"]
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error_message, programmingLanguage="sql"
            )
        elif node_status in ("success", "pass"):
            event_type = RunState.COMPLETE
        else:
            self.logger.info("Node %s has an unknown node status %s", node_unique_id, node_status)

        inputs = [
            self.node_to_dataset(node=model_input, has_facets=True)
            for model_input in self._get_model_inputs(node_unique_id)
        ]
        outputs = []
        if node := self._get_model_node(node_unique_id):
            outputs = [self.node_to_output_dataset(node=node, has_facets=True)]

        if resource_type == "test":
            success = node_status == "pass"
            if assertion := self._get_assertion(node_unique_id, success):
                assertion_facet = dq.DataQualityAssertionsDatasetFacet(assertions=[assertion])
                inputs = []
                if attached_dataset := self._get_attached_dataset(node_unique_id):
                    dataset_facets = attached_dataset.facets
                    dataset_facets["dataQualityAssertions"] = assertion_facet
                    inputs = [
                        Dataset(
                            name=attached_dataset.name,
                            namespace=attached_dataset.namespace,
                            facets=dataset_facets,
                        )
                    ]

        return generate_run_event(
            event_type=event_type,
            event_time=node_finished_at,
            run_id=run_id,
            run_facets=run_facets,
            job_name=job_name,
            job_namespace=self.job_namespace,
            job_facets=job_facets,
            inputs=inputs,  # type: ignore[arg-type]
            outputs=outputs,
        )

    @handle_keyerror
    def _get_attached_dataset(self, test_node_id: str) -> Optional[Dataset]:
        """
        gets the input the data test is attached to.
        Some nodes like tests related to sources have an attached_node to None.
        """
        all_nodes = {**self.compiled_manifest["nodes"], **self.compiled_manifest["sources"]}
        test_node = all_nodes[test_node_id]
        attached_node_id = test_node["attached_node"]
        input_dataset = None
        if attached_node_id:
            attached_model_node = self._get_model_node(attached_node_id)
            input_dataset = self.node_to_dataset(node=attached_model_node, has_facets=True)
        return input_dataset

    @handle_keyerror
    def _get_assertion(self, node_id: str, success: bool) -> Optional[dq.Assertion]:
        manifest_test_node = self.compiled_manifest["nodes"][node_id]
        name = manifest_test_node["test_metadata"]["name"]
        return dq.Assertion(
            assertion=name,
            success=success,
            column=get_from_nullable_chain(manifest_test_node["test_metadata"], ["kwargs", "column_name"]),
        )

    def _parse_sql_query_event(self, event) -> RunEvent:
        node_unique_id = get_node_unique_id(event)
        node_start_run_id = self.node_id_to_ol_run_id[node_unique_id]
        sql_ol_run_id = str(generate_new_uuid())
        sql_start_at = event["info"]["ts"]

        parent_run = ParentRunMetadata(
            run_id=node_start_run_id,
            job_name=self._get_job_name(event),
            job_namespace=self.job_namespace,
            root_parent_run_id=self.get_root_parent_run_id(),
            root_parent_job_namespace=self.get_root_parent_job_namespace(),
            root_parent_job_name=self.get_root_parent_job_name(),
        )

        run_facets = {
            **self.dbt_version_facet(),
            **self.processing_engine_facet(),
            **self.dbt_run_run_facet(),
            "parent": parent_run.to_openlineage(),
        }

        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType=get_job_type(event),
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            ),
            "sql": sql_job.SQLJobFacet(query=event["data"]["sql"], dialect=self.extract_dialect()),
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

        # We are heavily relying here on the fact that sql events for sql executions for
        # given node are run sequentially, as there is no information in a log
        # event that would allow us to distinguish different sql events for a single model
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
                message=error_message, programmingLanguage="sql", stackTrace=stacktrace
            )
        elif dbt_node_type == "SQLQueryStatus":
            run_state = RunState.COMPLETE

            if query_id := get_from_nullable_chain(event, ["data", "query_id"]):
                run_facets["externalQuery"] = external_query_run.ExternalQueryRunFacet(
                    externalQueryId=query_id, source="source"
                )

        return generate_run_event(
            event_type=run_state,
            event_time=event_time,
            run_id=sql_ol_run_event.run.runId,
            run_facets=run_facets,
            job_name=sql_ol_run_event.job.name,
            job_namespace=sql_ol_run_event.job.namespace,
            job_facets=sql_ol_run_event.job.facets,
        )

    def _parse_command_completed_event(self, event) -> RunEvent:
        success = event["data"]["success"]
        event_time = get_event_timestamp(event["data"]["completed_at"])
        run_facets = {
            **self.dbt_version_facet(),
            **self.processing_engine_facet(),
            **self.dbt_run_run_facet(),
        }

        parent_run_metadata = get_parent_run_metadata()
        if parent_run_metadata:
            run_facets["parent"] = parent_run_metadata.to_openlineage()

        if success:
            run_state = RunState.COMPLETE
        else:
            run_state = RunState.FAIL
            error_message = event["info"]["msg"]
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error_message, programmingLanguage="sql"
            )

        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType=get_job_type(event),
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            )
        }

        return generate_run_event(
            event_type=run_state,
            event_time=event_time,
            run_id=self.dbt_run_metadata.run_id,
            job_name=self.dbt_run_metadata.job_name,
            job_namespace=self.dbt_run_metadata.job_namespace,
            job_facets=job_facets,
            run_facets=run_facets,
        )

    def _setup_dbt_run_metadata(self, start_event):
        """
        The dbt command defines the parent run metadata of
        all subsequent OL events (NodeStart, NodeFinish ...)
        """
        root_parent_run_id = start_event.run.runId
        root_parent_job_name = start_event.job.name
        root_parent_job_namespace = start_event.job.namespace
        if start_event.run.facets.get("parent"):
            parent_facet = start_event.run.facets.get("parent")
            if hasattr(parent_facet, "root"):
                root_parent_run_id = parent_facet.root.run.runId
                root_parent_job_name = parent_facet.root.job.name
                root_parent_job_namespace = parent_facet.root.job.namespace
            else:
                root_parent_run_id = parent_facet.run.runId
                root_parent_job_name = parent_facet.job.name
                root_parent_job_namespace = parent_facet.job.namespace
        self.dbt_run_metadata = ParentRunMetadata(
            run_id=start_event.run.runId,
            job_name=start_event.job.name,
            job_namespace=start_event.job.namespace,
            root_parent_run_id=root_parent_run_id,
            root_parent_job_name=root_parent_job_name,
            root_parent_job_namespace=root_parent_job_namespace,
        )

    # TODO: remove after deprecation period
    def dbt_version_facet(self) -> Dict[str, DbtVersionRunFacet]:
        if not self.dbt_version:
            return {}
        self.logger.debug(
            "dbt_version facet is deprecated, and will be removed in future versions. "
            "Use processing_engine facet instead."
        )
        return {"dbt_version": DbtVersionRunFacet(version=self.dbt_version)}

    def dbt_run_run_facet(self) -> Dict[str, DbtRunRunFacet]:
        if not self.invocation_id:
            return {}
        return {
            "dbt_run": DbtRunRunFacet(
                invocation_id=self.invocation_id,
                project_name=self.project_name,
                project_version=self.project_version,
                profile_name=self.profile_name,
                dbt_runtime="core",
            )
        }

    def processing_engine_facet(self) -> Dict[str, processing_engine_run.ProcessingEngineRunFacet]:
        if not self.dbt_version:
            return {}
        return {
            "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                name="dbt",
                version=self.dbt_version,
                openlineageAdapterVersion=openlineage_version,
            )
        }

    def _get_sql_query_id(self, timestamp: str, node_id: str) -> int:
        """
        Not all adapters have the sql id defined in their dbt event.
        A node is executed by a single thread which means that sql queries of a single node are executed
        sequentially and their status is also reported sequentially.
        This function gives us a surrogate query id. It's auto-incremented.
        It gives the same id for the couple node_id, timestamp.
        """
        ids = self.node_id_to_sql_query_id[node_id]
        if timestamp not in ids:
            next_id = ids["next_id"]
            ids[timestamp] = next_id
            ids["next_id"] += 1

        return ids[timestamp]

    def _get_sql_job_name(self, event) -> str:
        """
        The name of the sql job is as follows
        {node_job_name}.sql.{incremental_id}
        """
        timestamp_str = event["info"]["ts"]
        node_unique_id = get_node_unique_id(event)
        query_id = self._get_sql_query_id(timestamp_str, node_unique_id)
        job_name = f"{node_unique_id}.sql.{query_id}"

        return job_name

    def _get_job_name(self, event) -> str:
        """
        The job name of models, snapshots, tests and others.
        """
        return get_node_unique_id(event)

    def _run_dbt_command(self) -> Generator[str, None, None]:
        """
        This executes the dbt command.
        Logs from the dbt log file are consumed in json format.
        """
        dbt_command_line = add_command_line_args(
            self.dbt_command_line,
            arg_names=["--log-format-file", "--log-level-file", "--log-path", "--log-file-max-bytes"],
            arg_values=["json", "debug", self.dbt_log_dirname, DBT_LOG_FILE_MAX_BYTES],
        )
        dbt_command_line = add_or_replace_command_line_option(
            dbt_command_line, option="--write-json", replace_option="--no-write-json"
        )

        self._open_dbt_log_file()
        self.logger.debug("Opened log file %s", self.dbt_log_file_path)

        incremental_reader = IncrementalFileReader(self._dbt_log_file)  # type: ignore[arg-type]
        last_size = os.stat(self.dbt_log_file_path).st_size
        self.logger.debug("Running dbt command: %s", " ".join(dbt_command_line))

        process = subprocess.Popen(dbt_command_line, stdout=sys.stdout, stderr=sys.stderr, text=True)
        parse_manifest = True
        last_log = datetime.datetime.now()

        try:
            while process.poll() is None:
                if (datetime.datetime.now() - last_log) >= datetime.timedelta(seconds=10):
                    self.logger.debug("dbt process is still running: waiting for logs to appear")
                    last_log = datetime.datetime.now()
                if parse_manifest and has_lines(self._dbt_log_file) > 0:  # type: ignore[arg-type]
                    # Load the manifest as soon as it exists
                    self.compiled_manifest
                    parse_manifest = False
                    self.logger.debug("Parsed manifest file")

                current_size = os.stat(self.dbt_log_file_path).st_size
                if current_size > last_size:
                    self.logger.debug(
                        "Current size: %d, last size: %d, to read: %d",
                        current_size,
                        last_size,
                        current_size - last_size,
                    )
                    yield from incremental_reader.read_lines(current_size - last_size)
                    last_size = current_size
                time.sleep(0.01)

            self.logger.debug("dbt process has exited. Waiting for logs to be flushed")
            max_loops = 10
            i = 0
            while (
                self._dbt_log_file is not None and not self.received_dbt_command_completed and i < max_loops
            ):
                i += 1
                current_size = os.stat(self.dbt_log_file_path).st_size
                if current_size > last_size:
                    self.logger.debug(
                        "Current size: %d, last size: %d, to read: %d",
                        current_size,
                        last_size,
                        current_size - last_size,
                    )
                    yield from incremental_reader.read_lines(current_size - last_size)
                    last_size = current_size
            self.logger.debug("Finished waiting for logs to be flushed")

        except Exception:
            self.logger.exception(
                "An exception occurred in OpenLineage dbt wrapper. "
                "It should not affect underlying dbt execution that is still running."
            )
            last_log = datetime.datetime.now()

            while process.poll() is None:
                if (datetime.datetime.now() - last_log) >= datetime.timedelta(seconds=10):
                    self.logger.debug("dbt process is still running. Further logs won't be processed")
                    last_log = datetime.datetime.now()
                time.sleep(0.1)
                pass
            self.logger.debug("dbt process has finished")
        finally:
            if self._dbt_log_file is not None:
                self._dbt_log_file.close()
                if self.is_random_logfile:
                    try:
                        self.logger.debug("Removing log file: %s", self.dbt_log_file_path)
                        os.remove(self.dbt_log_file_path)
                    except Exception:
                        self.logger.warning(
                            "Failed to remove log file: %s. Leftover logs can consume disk space.",
                            self.dbt_log_file_path,
                        )
            self.dbt_command_return_code = process.returncode

    def _open_dbt_log_file(self):
        """
        If the log file already exists when the dbt command is executed logs are appended.
        This reads all the lines on first (and only) open to get rid of those previous lines.
        If the file doesn't exist it creates an empty one
        """
        if self._dbt_log_file is not None:
            return

        if not os.path.exists(self.dbt_log_file_path):
            logs_directory = os.path.dirname(self.dbt_log_file_path)
            os.makedirs(logs_directory, exist_ok=True)
            with open(self.dbt_log_file_path, "w"):
                pass

        self._dbt_log_file = open(self.dbt_log_file_path)
        while self._dbt_log_file.readlines():
            pass

    @handle_keyerror
    def _get_model_node(self, node_id) -> Optional[ModelNode]:
        """
        Builds a ModelNode of a given node_id
        """
        if node_id in self.compiled_manifest["nodes"]:
            node_type = "model"
            manifest_node = self.compiled_manifest["nodes"][node_id]
        elif node_id in self.compiled_manifest["sources"]:
            node_type = "source"
            manifest_node = self.compiled_manifest["sources"][node_id]
        else:
            raise RuntimeError(f"{node_id} not found in nodes or sources")
        catalog_node = get_from_nullable_chain(self.catalog, ["nodes", node_id])
        return ModelNode(type=node_type, metadata_node=manifest_node, catalog_node=catalog_node)

    @handle_keyerror
    def _get_node_tags(self, node_id: str) -> List[str]:
        """
        Extract tags from a dbt node in the compiled manifest
        """
        all_nodes = {**self.compiled_manifest["nodes"], **self.compiled_manifest["sources"]}
        manifest_node = all_nodes[node_id]
        return manifest_node.get("tags", [])

    def _get_model_inputs(self, node_id) -> List[ModelNode]:
        """
        Builds the model's upstream inputs
        """
        if node_id in self.node_id_to_inputs:
            return self.node_id_to_inputs[node_id]

        try:
            input_node_ids = self.compiled_manifest["parent_map"][node_id]
            inputs = [node for n in input_node_ids if (node := self._get_model_node(n)) is not None]
            self.node_id_to_inputs[node_id] = inputs
            return inputs
        except KeyError:
            self.logger.warning(
                "Did not find node_id %s in the dbt manifest - did "
                "the manifest change while the job was executing?",
                node_id,
            )
        return []

    def _validate_manifest_integrity(self) -> ManifestIntegrityResult:
        """
        Performs metadata integrity checks on the compiled manifest.

        Validates that all nodes referenced in parent_map exist in the manifest's
        node definitions (either in 'nodes' or 'sources' sections).

        Returns:
            ManifestIntegrityResult: Validation results with integrity check information.
        """
        if not self._compiled_manifest:
            return ManifestIntegrityResult(
                is_valid=True,
                missing_nodes=[],
                missing_parents=[],
                missing_children=[],
                total_missing=0,
                parent_map_size=0,
                available_nodes=0,
            )

        parent_map = self._compiled_manifest.get("parent_map", {})
        all_nodes = {**self._compiled_manifest.get("nodes", {}), **self._compiled_manifest.get("sources", {})}

        missing_nodes = []
        missing_parents = []
        missing_children = []

        for parent_node_id, child_node_ids in parent_map.items():
            # Check that the parent node exists
            if parent_node_id not in all_nodes:
                missing_nodes.append(parent_node_id)
                missing_parents.append(parent_node_id)

            # Check that all child nodes exist
            for child_node_id in child_node_ids:
                if child_node_id not in all_nodes:
                    missing_nodes.append(child_node_id)
                    missing_children.append(child_node_id)

        # Remove duplicates while preserving order
        missing_nodes = list(dict.fromkeys(missing_nodes))
        missing_parents = list(dict.fromkeys(missing_parents))
        missing_children = list(dict.fromkeys(missing_children))

        if missing_nodes:
            self.logger.warning(
                "Manifest integrity check failed: Found %d nodes in parent_map that don't exist "
                "in manifest node definitions, printing first %d of them: %s",
                len(missing_nodes),
                min(10, len(missing_nodes)),
                missing_nodes[: min(10, len(missing_nodes))],  # Log first 10 to avoid overwhelming logs
            )

        return ManifestIntegrityResult(
            is_valid=len(missing_nodes) == 0,
            missing_nodes=missing_nodes,
            missing_parents=missing_parents,
            missing_children=missing_children,
            total_missing=len(missing_nodes),
            parent_map_size=len(parent_map),
            available_nodes=len(all_nodes),
        )

    def get_dbt_metadata(
        self,
    ):
        """
        Replaced by properties
        """
        pass

    def get_root_parent_run_id(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_run_id
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_run_id
        return None

    def get_root_parent_job_namespace(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_job_namespace
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_job_namespace
        return None

    def get_root_parent_job_name(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_job_name
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_job_name
        return None
