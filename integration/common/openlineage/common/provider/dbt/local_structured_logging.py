# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import itertools
#import re
import sys
import hashlib, base64
import subprocess
from functools import cached_property
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, TypeVar

#from jinja2 import Environment, Undefined
import tempfile

#from mypyc.irbuild.format_str_tokenizer import unique

#from integration.common.tests.dbt.test_dbt_local import parent_run_metadata
#from mypyc.ir.ops import Value
#from openlineage.common.utils import get_from_nullable_chain

from openlineage.client.uuid import generate_static_uuid, generate_new_uuid
from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor
from openlineage.client.event_v2 import Dataset, InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.common.utils import parse_single_arg
from openlineage.client.facet_v2 import (
    #BaseFacet,
    #DatasetFacet,
    #InputDatasetFacet,
    #JobFacet,
    #OutputDatasetFacet,
    #data_quality_assertions_dataset,
    #datasource_dataset,
    #documentation_dataset,
    job_type_job,
    sql_job,
    error_message_run
    #output_statistics_output_dataset,
    #parent_run,
    #schema_dataset,
    #sql_job,
)

from openlineage.common.provider.dbt.processor import ParentRunMetadata

openlineage_logger = logging.getLogger("openlineage.dbt")
openlineage_logger.setLevel(os.getenv("OPENLINEAGE_DBT_LOGGING", "INFO"))
openlineage_logger.addHandler(logging.StreamHandler(sys.stdout))
# deprecated dbtol logger
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

# when executing models
# Full list of adapter events here https://github.com/dbt-labs/dbt-adapters/blob/d56ca3494c51cbb9c1546e207626f3444be468cc/dbt/adapters/events/types.py#L4
# And for dbt core events here https://github.com/dbt-labs/dbt-core/blob/fd6ec71dabae91f4f243362acbcda6dc1beccec6/core/dbt/events/types.py#L1349
#NON_RELEVANT_EVENTS = [
#    "LogStartLine", "NodeCompiling", "WritingInjectedSQLForNode", "JinjaLogDebug",
#    "ConnectionUsed", "NewConnectionOpening", "LogModelResult", "SQLCommit"
#]
#
RELEVANT_EVENTS = [
    "MainReportVersion", "NodeStart", "SQLQuery", "SQLQueryStatus", "NodeFinished", "CatchableExceptionOnRun"
]
    
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
        inputs: Optional[list[InputDataset]] = None,
        outputs: Optional[list[InputDataset]] = None,
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
        outputs=outputs
        # producer=PRODUCER, #todo common to all dbt
    )


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
        # todo the full location is given by https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles#advanced-customizing-a-profile-directory
        # below we just rely on the cli. #todo maybe put in the script entrypoint
        self.profiles_dir = parse_single_arg(self.dbt_command, ["--profiles-dir"])


        # below can be replaced with lru_cache functions
        # for online ol events
        # example dim.foo -> ol run 1234-1234-1234
        self.node_id_to_ol_run_id = {}
        # sql hash -> ol run 1234-1234-1234
        self.node_id_to_sql_query_id = {}
        # The SQL events are reported by a givena thread. What happens when multiple threads execute the command ?
        # a node_id is executed by a single thread only
        # node_id -> SQL OL event Start, do we need the same for node lifecycle events
        self.node_id_to_sql_start_event = {}



    @cached_property
    def compile_manifest_path(self):
        ol_compile_target = f"ol-compile-target-{str(generate_new_uuid())}" #todo can be deterministic
        return os.path.join(self.absolute_dir, ol_compile_target, "manifest.json")

    @cached_property
    def command(self):
        """
        should it be property ?
        naming ?
        """
        return get_dbt_command(self.dbt_command)

    # list of RunEvents instead
    def parse(self) -> List[RunEvent]:
        """
        alternative:
        1. report on the execution of ol events and send the manifest info at the end. W'll lose the realtime monitoring

        algo:
        1. compile manifest
        2. run the command
        3. consume the logs and send the events
        """

        if self.command not in HANDLED_COMMANDS:
            # do nothing
            ...

        self.extract_adapter_type(self.profile)
        self.extract_dataset_namespace(self.profile)

        self._compile_manifest()

        for line in self._run_dbt_command():
            logger.info(line)
            ol_event = self._parse_structured_log_event(line)
            logger.info(f"### ol_event={ol_event}")
            if ol_event:
                yield ol_event

    def _get_parent_run_metadata(self):
        parent_id = os.getenv("OPENLINEAGE_PARENT_ID")
        parent_run_metadata = None
        if parent_id:
            parent_namespace, parent_job_name, parent_run_id = parent_id.split("/")
            parent_run_metadata = ParentRunMetadata(
                run_id=parent_run_id,
                job_name=parent_job_name,
                job_namespace=parent_namespace)
        return parent_run_metadata

    def _parse_dbt_start_command_event(self, event):
        parent_run_metadata = self._get_parent_run_metadata()
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
        For the subsequent ol events
        """
        self.dbt_run_metadata = ParentRunMetadata( # it's run Metadata, why name it parent ?
            run_id=start_event.run.runId,
            job_name=start_event.job.name,
            job_namespace=start_event.job.namespace,
        )


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
            return None # do something else. Log the exception

        dbt_event_name = dbt_event["info"]["name"]

        # only happens once
        if dbt_event_name == "MainReportVersion":
            start_event = self._parse_dbt_start_command_event(dbt_event)
            self._setup_dbt_run_metadata(start_event)
            return start_event

        elif dbt_event_name == "CommandCompleted":
            end_event = self._parse_command_completed_event(dbt_event)
            return end_event

        try:
            dbt_event["data"]["node_info"]["unique_id"]
        except KeyError:
            return None

        logger.info(f"##### dbt_event_name={dbt_event_name}")
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
        node_unique_id = event["data"]["node_info"]["unique_id"]
        if node_unique_id not in self.node_id_to_ol_run_id:
            self.node_id_to_ol_run_id[node_unique_id] = str(generate_new_uuid())

        run_id = self.node_id_to_ol_run_id[node_unique_id]
        node_start_time = event["data"]["node_info"]["node_started_at"]

        #todo what if it's null ??
        parent_run_metadata = self.dbt_run_metadata.to_openlineage()
        run_facets = {"parent": parent_run_metadata}


        job_name = self._get_job_name(event)
        # todo define types of vars (int, str ...)
        job_facets = { "jobType": job_type_job.JobTypeJobFacet(
            jobType=self._get_job_type(event),
            integration="DBT",
            processingType="BATCH",
            producer=self.producer,
        )}

        return generate_run_event(
            event_type=RunState.START,
            event_time=node_start_time,
            run_id=run_id,
            run_facets=run_facets,
            job_name=job_name,
            job_namespace=self.job_namespace,
            job_facets=job_facets
        )

    def _parse_node_finish_event(self, event):
        node_unique_id = event["data"]["node_info"]["unique_id"]
        node_finished_at = event["data"]["node_info"]["node_finished_at"]
        node_status = event["data"]["node_info"]["node_status"]
        run_id = self.node_id_to_ol_run_id[node_unique_id]

        #todo what if it's None ???
        parent_run_metadata = self.dbt_run_metadata.to_openlineage()
        run_facets = {"parent": parent_run_metadata}

        job_name = self._get_job_name(event)

        job_facets = { "jobType": job_type_job.JobTypeJobFacet(
            jobType=self._get_job_type(event),
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


        return generate_run_event(
            event_type=event_type,
            event_time=node_finished_at,
            run_id=run_id,
            run_facets=run_facets,
            job_name=job_name,
            job_namespace=self.job_namespace,
            job_facets=job_facets
        )

    def _get_sql_query_id(self, node_id):
        """
        not all adapters have the sql id defined in the dbt event.
        a node is executed by a single thread which means that sql queries of a single node are executed
        sequentially and their status is also reported sequentially.
        this function gives us a surrogate query id
        """
        query_id = None
        if node_id not in self.node_id_to_sql_query_id:
            self.node_id_to_sql_query_id[node_id] = 1

        query_id = self.node_id_to_sql_query_id[node_id]
        self.node_id_to_sql_query_id[node_id] += 1
        return query_id



    def _parse_sql_query_event(self, event):
        """
        #todo how do we distinguish two successive same SQL queries ?? a node is executed by a single thread at a time
        # a node_id is executed by a single node and SQL queries are executed sequentially
        """
        node_unique_id = self._get_node_unique_id(event)
        parent_run_id = self.node_id_to_ol_run_id[node_unique_id]
        sql_ol_run_id = str(generate_new_uuid())
        sql_start_at = event["info"]["ts"]


        # run of the NodeStart event
        parent_run = ParentRunMetadata(
            run_id=parent_run_id,
            job_name= self._get_job_name(event),
            job_namespace=self.job_namespace
        )

        run_facets = {"parent": parent_run.to_openlineage()}

        job_facets = { "jobType": job_type_job.JobTypeJobFacet(
            jobType=self._get_job_type(event),
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
            job_name=self._get_sql_job_name(event), # added sql to this. We may want to change it.
            job_namespace=self.job_namespace,
            job_facets=job_facets
        )

        # to get the status after
        # todo we should have the query_id here !!!! Snowflake adapter has it but not the postgress one.
        # snowflake has it only for the query of the model. not for all queries.
        # We should have an upstream dbt PR to add the query_id in all responses
        self.node_id_to_sql_start_event[node_unique_id] = sql_event

        return sql_event

    def _parse_sql_query_status_event(self, event):
        """
        we need to find the start event of this sql status event
        there are no multiple SQLs that are executed for the same node

        we need to have the last SQL run event sent
        this will read different types of nodes
        1. SQLStatus
        2. CatchableExceptionOnRun
        3. LogMessages

        This will get ignored if there is no active dbt model running
        """
        # todo we still need to ensure that there is a SQLQuery Node being processed

        node_unique_id = self._get_node_unique_id(event)
        sql_ol_run_event = self.node_id_to_sql_start_event[node_unique_id] # todo are we sure this corresponds to the same query ? Yes

        dbt_node_type = event["info"]["name"]
        run_state = RunState.OTHER
        event_time = event["info"]["ts"] # todo is it always UTC here ?
        run_facets = sql_ol_run_event.run.facets
        if dbt_node_type == "CatchableExceptionOnRun":
            run_state = RunState.FAIL
            # get the error message
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
            run_facets=run_facets, #todo is it necessary to have the same run facets/job facets ??
            job_name=sql_ol_run_event.job.name, # added sql to this. We may want to change it.
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

        parent_run_metadata = self._get_parent_run_metadata()

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

    def _get_node_unique_id(self, event):
        return event["data"]["node_info"]["unique_id"]

    def _get_sql_job_name(self, event):
        """
        #todo subject to change or we can send executing events of the same node with only sql facets
        # executing events <==> RunEvent.execute
        """
        node_job_name = self._get_job_name(event)
        sql_query = event["data"]["sql"]
        node_started_at = event["data"]["node_info"]["node_started_at"]
        node_unique_id = node_unique_id = event["data"]["node_info"]["unique_id"]
        query_id = self._get_sql_query_id(node_unique_id)
        job_name = f"{node_job_name}.sql.{query_id}"
        return job_name

    def _get_job_name(self, event):
        database = event["data"]["node_info"]["node_relation"]["database"]
        schema = event["data"]["node_info"]["node_relation"]["schema"]
        node_unique_id = event["data"]["node_info"]["unique_id"]
        node_id = self.removeprefix(node_unique_id, "model.") # todo remove prefix for "snapshot." as well
        suffix = ".build.run" if self.command == "build" else "" #todo why do we have this ?

        return f"{database}.{schema}.{node_id}{suffix}"

    def _get_job_type(self, event):
        node_unique_id = event["data"]["node_info"]["unique_id"]
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
        This is a generator, it returns log lines
        :return:
        """
        # add the --log format
        dbt_command = self.dbt_command + ["--log-format", "json"]
        stdout_file = tempfile.NamedTemporaryFile(delete=False)
        stderr_file = tempfile.NamedTemporaryFile(delete=False)
        stdout_reader = open(stdout_file.name, mode="r")
        stderr_reader = open(stderr_file.name, mode="r")
        process = subprocess.Popen(dbt_command, stdout=stdout_reader, stderr=stderr_reader)
        while process.poll() is None:
            for line in stdout_reader.readlines():
                line = line.strip()
                if line:
                    yield line
            # do something else : continue / stop in case of error
            for line in stderr_reader.readlines():
                line = line.strip()
                if line:
                    logger.error(line)



    def _compile_manifest(self) -> str:
        """
        only need to compile the manifest to get the inputs/outputs
        we get the manifest in some cases
        todo: only in certain command the manifest is produced
        """
        if not os.path.isfile(self.compile_manifest_path) and self._produces_run_result_file():
            compile_command = self._get_compile_command()
            process = subprocess.Popen(compile_command, stdout=sys.stdout, stderr=sys.stderr)
            process.wait()
            #todo add logging ?



    def _produces_run_result_file(self):
        """
        True if the command produces a run_result.json file
        https://docs.getdbt.com/reference/artifacts/run-results-json
        """
        dbt_command = set(self.dbt_command)
        for command in RUN_RESULT_COMMANDS:
            if command in dbt_command:
                return True
        return False



    def _get_compile_command(self):
        """
        get a run command and gives us the compile command to generate the manifest.
        :param dbt_command:
        :return:
        """
        if not self._produces_run_result_file():
            raise ValueError("This command is not a command that produces a run_result.json file")

        # in a different target
        compile_command = None
        for i, token, run_command in itertools.product(enumerate(self.dbt_command), RUN_RESULT_COMMANDS):
            if token == run_command:
                compile_command = list(self.dbt_command)
                compile_command[i] = "compile"
                break

        # change the target
        ol_compile_target = self.compile_manifest_path.split("/")[-2]
        compile_command = compile_command + ["--target-path", ol_compile_target] # overrides previous value

        return compile_command


    def _execute_dbt_cmd(self):
        ...



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

        return profile_dict["outputs"][self.target]

    @cached_property
    def compiled_manifest(self):
        """
        gets the compiled manifest for the the dbt command
        """
        if not os.path.isfile(self.compile_manifest_path):
            self._compile_manifest()

        return self.load_metadata(self.compile_manifest_path, [2, 3, 4, 5, 6, 7], self.logger)



    def get_dbt_metadata(
            self,
    ) -> Tuple[Dict[Any, Any], Optional[Dict[Any, Any]], Dict[Any, Any], Optional[Dict[Any, Any]]]:
        """
        replaced by the other cached properties
        """
        pass
