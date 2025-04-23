# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime
import json
import os
import subprocess
import sys
import time
from collections import defaultdict
from functools import cached_property
from typing import Dict, Generator, List, Optional, TextIO

from openlineage.client.event_v2 import Dataset, RunEvent, RunState
from openlineage.client.facet_v2 import (
    data_quality_assertions_dataset,
    error_message_run,
    job_type_job,
    sql_job,
)
from openlineage.client.run import InputDataset
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
    get_dbt_log_path,
    get_dbt_profiles_dir,
    get_event_timestamp,
    get_job_type,
    get_node_unique_id,
    get_parent_run_metadata,
)
from openlineage.common.utils import (
    IncrementalFileReader,
    add_command_line_args,
    add_or_replace_command_line_option,
    get_from_nullable_chain,
    has_lines,
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

        self.dbt_command_line: List[str] = dbt_command_line
        self.profiles_dir: str = get_dbt_profiles_dir(command=self.dbt_command_line)
        self.dbt_log_file_path: str = get_dbt_log_path(command=self.dbt_command_line)
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
        self._dbt_log_file: Optional[TextIO] = None
        self.received_dbt_command_completed = False

    @cached_property
    def dbt_command(self) -> str:
        return get_dbt_command(self.dbt_command_line)

    @property
    def dbt_version(self) -> Optional[str]:
        """
        Extracted from the first structured log MainReportVersion
        """
        return self._dbt_version

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
            return self._compiled_manifest
        else:
            return {}

    def parse(self) -> Generator[RunEvent, None, None]:
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
            yield self._get_dbt_command_abort_event()

    def _parse_structured_log_event(self, line: str) -> Optional[RunEvent]:
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
            self.logger.error(f"The following log is not valid JSON:\n{line}")
            return None

        dbt_event_name = dbt_event["info"]["name"]

        if dbt_event_name == "MainReportVersion":
            self._dbt_version = dbt_event["data"]["version"][1:]
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
        run_facets = self.dbt_version_facet()
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
        run_facets = self.dbt_version_facet()
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

    def parse_node_finished_event(self, event) -> RunEvent:
        resource_type = event["data"]["node_info"]["resource_type"]
        node_unique_id = get_node_unique_id(event)
        node_finished_at = get_event_timestamp(event["data"]["node_info"]["node_finished_at"])
        if node_finished_at == "":
            node_finished_at = get_event_timestamp(event["data"]["node_info"]["node_started_at"])
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
            self.logger.info(f"node {node_unique_id} has an unknown node status {node_status}")

        inputs = [
            self.node_to_dataset(node=model_input, has_facets=True)
            for model_input in self._get_model_inputs(node_unique_id)
        ]
        outputs = [self.node_to_output_dataset(node=self._get_model_node(node_unique_id), has_facets=True)]
        if resource_type == "test":
            success = node_status == "pass"
            assertion = self._get_assertion(node_unique_id, success)
            assertion_facet = data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
                assertions=[assertion]
            )
            attached_dataset = self._get_attached_dataset(node_unique_id)
            inputs = []
            if attached_dataset:
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
            inputs=inputs,
            outputs=outputs,
        )

    def _get_attached_dataset(self, test_node_id: str) -> Optional[InputDataset]:
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

    def _get_assertion(self, node_id: str, success: bool) -> data_quality_assertions_dataset.Assertion:
        manifest_test_node = self.compiled_manifest["nodes"][node_id]
        name = manifest_test_node["test_metadata"]["name"]
        return data_quality_assertions_dataset.Assertion(
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
                message=error_message, programmingLanguage="sql", stackTrace=stacktrace
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

    def _parse_command_completed_event(self, event) -> RunEvent:
        success = event["data"]["success"]
        event_time = get_event_timestamp(event["data"]["completed_at"])
        run_facets = self.dbt_version_facet()
        if success:
            run_state = RunState.COMPLETE
        else:
            run_state = RunState.FAIL
            error_message = event["info"]["msg"]
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error_message, programmingLanguage="sql"
            )

        parent_run_metadata = get_parent_run_metadata()

        if parent_run_metadata:
            run_facets["parent"] = parent_run_metadata.to_openlineage()

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

    def dbt_version_facet(self) -> DbtVersionRunFacet:
        return {"dbt_version": DbtVersionRunFacet(version=self.dbt_version)}

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
            arg_names=["--log-format-file", "--log-level-file"],
            arg_values=["json", "debug"],
        )
        dbt_command_line = add_or_replace_command_line_option(
            dbt_command_line, option="--write-json", replace_option="--no-write-json"
        )
        self._open_dbt_log_file()
        incremental_reader = IncrementalFileReader(self._dbt_log_file)
        process = subprocess.Popen(dbt_command_line, stdout=sys.stdout, stderr=sys.stderr, text=True)
        parse_manifest = True
        try:
            while process.poll() is None:
                if parse_manifest and has_lines(self._dbt_log_file) > 0:
                    # Load the manifest as soon as it exists
                    self.compiled_manifest
                    parse_manifest = False

                yield from incremental_reader.read_lines()
                time.sleep(0.1)

            max_loops = 10
            i = 0
            while (
                self._dbt_log_file is not None and not self.received_dbt_command_completed and i < max_loops
            ):
                yield from incremental_reader.read_lines()
                time.sleep(0.1)
                i += 1

        except Exception:
            self.logger.exception("An exception occurred in OL code. dbt is still running.")
            while process.poll() is None:
                pass  # wait for the process to finish
        finally:
            if self._dbt_log_file is not None:
                self._dbt_log_file.close()

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

    def get_root_parent_run_id(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_run_id
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_run_id

    def get_root_parent_job_namespace(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_job_namespace
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_job_namespace

    def get_root_parent_job_name(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_job_name
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_job_name
