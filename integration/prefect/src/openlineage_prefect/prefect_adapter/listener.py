# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import asyncio
import dataclasses
import json
import logging
import os
import re
from datetime import datetime, timezone

import requests
from openlineage.client.run import RunState
from openlineage.client.uuid import generate_static_uuid
from openlineage_prefect.prefect_adapter.adapter import (
    DatasetInfo,
    DeploymentInfo,
    FlowInfo,
    PrefectOpenLineageAdapter,
    TaskInfo,
)
from prefect.client.orchestration import get_client
from prefect.events.clients import get_events_subscriber
from prefect.events.filters import EventFilter, EventNameFilter, EventOccurredFilter
from prefect.events.schemas.events import Event
from prefect.exceptions import ObjectNotFound, PrefectHTTPStatusError

TASK_NAME_ERR_MSG = "TASK_NAME_TYPE must be either 'base' or 'full'."
TASK_NAME_TYPE = os.environ.get("TASK_NAME_TYPE") or os.environ.get("JOB_NAME_TYPE", "base")
if TASK_NAME_TYPE not in ("base", "full"):
    raise ValueError(TASK_NAME_ERR_MSG)
JOB_NAMESPACE: str = os.environ.get("OPENLINEAGE_NAMESPACE", "default")
FLOW_NAME_ERR_MSG = "FLOW_NAME_TYPE must be either 'flow' or 'flow-run'."
FLOW_NAME_TYPE: str = os.environ.get("FLOW_NAME_TYPE", "flow")
if FLOW_NAME_TYPE not in ("flow", "flow-run"):
    raise ValueError(FLOW_NAME_ERR_MSG)
PREFECT_API_URL: str = os.environ.get("PREFECT_API_URL", "http://localhost:4200/api")
STATUS_CODE_OK: int = 200


logger: logging.Logger = logging.getLogger(__name__)


class PrefectOpenLineageListener:
    def __init__(
        self,
        client=None,
        ol_adapter=None,
    ):
        self.client = client or get_client()
        self.ol_adapter = ol_adapter or PrefectOpenLineageAdapter()
        self.related_deployment_info: dict = {}
        self.filter_criteria = EventFilter(
            event=EventNameFilter(
                prefix=[
                    "prefect.task-run.",
                    "prefect.flow-run.",
                    "prefect.asset.materialization.",
                ]
            ),
            occurred=EventOccurredFilter(since=datetime.now(timezone.utc)),
        )

    def build_run_id(self, execution_time: datetime, run_name: str, namespace: str) -> str:
        """
        Builds a deterministic UUID for the OpenLineage run based on the execution
        time, run name, and namespace.
        """
        return str(
            generate_static_uuid(
                instant=execution_time,
                data=f"{namespace}.{run_name}".encode(),
            )
        )

    def get_base_name(self, run_name: str) -> str:
        """
        Removes auto-generated unique suffixes from a Prefect task run name.
        """
        return re.sub(r"-[a-f0-9]+$", "", run_name)

    async def get_deployment_info(self, flow_run_id: str) -> DeploymentInfo:
        """
        Retrieves the deployment information for a given flow run. When no deployment
        is found, the flow run and namespace are returned.
        """
        flow_run = await self.client.read_flow_run(flow_run_id)

        try:
            deployment = await self.client.read_deployment(flow_run.deployment_id)
            try:
                ns = deployment.job_variables["env"]["OPENLINEAGE_NAMESPACE"]
            except KeyError:
                ns = JOB_NAMESPACE
                logger.info(
                    "OPENLINEAGE_NAMESPACE deployment variable not found. Using \
                    OPENLINEAGE_NAMESPACE env variable."
                )
                if JOB_NAMESPACE == "default":
                    logger.info("OPENLINEAGE_NAMESPACE env variable not set. Namespace will be 'default.'")
            return DeploymentInfo(
                id=str(deployment.id),
                created=deployment.created,
                updated=deployment.updated,
                name=deployment.name,
                namespace=ns,
                flow_run=flow_run,
            )

        except (AttributeError, TypeError):
            logger.info("Deployment not found for flow run: %s", flow_run_id)
            ns = JOB_NAMESPACE
            logger.info(
                "OPENLINEAGE_NAMESPACE deployment variable not found. Using \
                OPENLINEAGE_NAMESPACE env variable for the namespace."
            )
            if JOB_NAMESPACE == "default":
                logger.info("OPENLINEAGE_NAMESPACE env variable not set. Namespace will be 'default.'")

            return DeploymentInfo(
                namespace=ns,
                flow_run=flow_run,
            )

    async def get_flow_info(self, flow_run) -> FlowInfo:
        """
        Retrieves the start time, name and namespace for a given flow run.
        """
        flow_id = flow_run.flow_id
        flow = await self.client.read_flow(flow_id)
        if FLOW_NAME_TYPE == "flow":
            flow_name = flow.name
        elif FLOW_NAME_TYPE == "flow-run":
            flow_name = flow_run.name

        return FlowInfo(
            start_time=flow_run.start_time,
            name=flow_name,
            namespace=None,
            event_time=None,
            state_msg=None,
        )

    async def get_prefect_version(self) -> str | None:
        """Retrieves the Prefect version from the Prefect API."""
        try:
            response = requests.get(f"{PREFECT_API_URL}/admin/version", timeout=(5, 20))
            return response.json()
        except TypeError:
            logger.info("Cannot get the Prefect version. Did you set the PREFECT_API_URL?")

    async def get_artifacts_by_task_run(self, run_id: str) -> list[dict]:
        """
        Retrieve artifacts associated with a given task run ID. Returns
        a list of dictionaries containing the URI, table, and dataset type
        for each artifact.
        """
        payload = {"artifacts": {"task_run_id": {"any_": [run_id]}}}
        response = requests.post(f"{PREFECT_API_URL}/artifacts/filter", timeout=(5, 20), json=payload)
        if response.status_code == STATUS_CODE_OK:
            dataset_info = []
            artifacts = response.json()
            for artifact in artifacts:
                if artifact["description"] and "ol-dataset" in artifact["description"]:
                    try:
                        dataset_type = artifact["description"].split("_")[-1].lower()
                        assert dataset_type in ["input", "output"], f"Invalid dataset_type: {dataset_type}"
                    except AssertionError:
                        logging.warning("Invalid dataset_type in artifact description.")
                        try:
                            dataset_type = artifact["description"].split("-")[-1].lower()
                            assert dataset_type in ["input", "output"], (
                                f"Invalid dataset_type: {dataset_type}"
                            )
                        except AssertionError:
                            logging.warning(
                                "Invalid dataset_type in artifact description after trying both separators."
                            )
                            dataset_type = None

                    data_list = json.loads(artifact["data"])
                    try:
                        uri = data_list[0]["database_uri"]
                    except AttributeError:
                        logging.warning("Failed to retrieve database_uri from artifact data.")
                        uri = None
                    try:
                        table = data_list[0]["table"]
                    except AttributeError:
                        logging.warning("Failed to retrieve table from artifact data.")
                        table = None
                    dataset_info.append({"uri": uri, "table": table, "dataset_type": dataset_type})
            return dataset_info
        logger.info("No datasets found for task run.")
        return []

    async def get_parent_runs(self, payload: dict, prefect_task_run_id: str) -> list[dict]:
        """Retrieves the Prefect parent task runs for a given task run."""

        parent_runs = []
        try:
            task_parents = payload["task_run"]["task_inputs"]["__parents__"]
            for parent in task_parents:
                task_run_id: str | None = parent["id"] if parent["input_type"] == "task_run" else None
                if task_run_id:
                    parent_run = await self.client.read_task_run(task_run_id)
                    if TASK_NAME_TYPE == "base":
                        parent_name = self.get_base_name(parent_run.name)
                    elif TASK_NAME_TYPE == "full":
                        parent_name = parent_run.name
                    parent_deployment_info = await self.get_deployment_info(parent_run.flow_run_id)
                    parent_run_id = self.build_run_id(
                        parent_run.start_time, parent_name, parent_deployment_info.namespace
                    )
                    parent_runs.append(
                        {
                            "name": parent_name,
                            "namespace": parent_deployment_info.namespace,
                            "id": parent_run_id,
                        }
                    )
                else:
                    logger.info("Parent task run ID not found for parent %s", parent)
        except KeyError:
            logger.info("No task parents found for %s", prefect_task_run_id)
        return parent_runs

    async def collect_and_process_flow_runs(
        self, prefect_version: str, event: Event, event_state: RunState
    ) -> None:
        """Retrieve the flow runs for a given event and emit OpenLineage events."""

        for res in event.related:
            if res["prefect.resource.role"] == "flow":
                flow_run_id = event.resource.id.split(".")[-1]
                prefect_state_msg = (
                    event.payload.get("validated_state", {}).get("message")
                    or event.resource.get("prefect.state-message")
                    or ""
                    if event_state in [RunState.FAIL, RunState.ABORT]
                    else ""
                )
                logging.info(prefect_state_msg)
                event_time: datetime = datetime.fromisoformat(event.resource["prefect.state-timestamp"])
                if event_state != RunState.START:
                    try:
                        related_deployment_info = self.related_deployment_info[flow_run_id]
                    except KeyError:
                        related_deployment_info = await self.get_deployment_info(flow_run_id)
                        self.related_deployment_info[flow_run_id] = related_deployment_info
                else:
                    related_deployment_info = await self.get_deployment_info(flow_run_id)
                    self.related_deployment_info[flow_run_id] = related_deployment_info
                flow_info = await self.get_flow_info(related_deployment_info.flow_run)
                try:
                    ol_flow_run_id: str = self.build_run_id(
                        flow_info.start_time,
                        flow_info.name,
                        self.related_deployment_info[flow_run_id].namespace,
                    )
                except AttributeError:
                    logger.info(
                        "Failed to build OpenLineage run id for Flow %s. Flow %s event will not be emitted.",
                        flow_run_id,
                        event_state,
                    )
                    continue

                flow_info = dataclasses.replace(
                    flow_info,
                    event_time=event_time,
                    namespace=self.related_deployment_info[flow_run_id].namespace,
                    state_msg=prefect_state_msg,
                )

                self.ol_adapter.create_and_emit_flow_event(
                    run_id=ol_flow_run_id,
                    event_type=event_state,
                    flow_info=flow_info,
                    prefect_version=prefect_version,
                    deployment=related_deployment_info if related_deployment_info.id else None,
                )

    async def collect_and_process_task_runs(
        self, prefect_version: str, event: Event, event_state: RunState
    ) -> None:
        """Retrieves the task runs for a given event and emits OpenLineage events."""

        event_time = event.occurred
        expected_start_time = event.payload["task_run"]["expected_start_time"]
        prefect_task_run_id = event.resource.id.split(".")[-1]
        raw_start_time = event.payload["task_run"].get("start_time")
        if not raw_start_time:
            logger.info(
                "No start time found for task run %s. An event will not be emitted.", prefect_task_run_id
            )
            return
        start_time = datetime.fromisoformat(raw_start_time.replace("Z", "+00:00"))
        prefect_state_msg = (
            event.payload.get("validated_state", {}).get("message")
            or event.resource.get("prefect.state-message")
            or ""
            if event_state in [RunState.FAIL, RunState.ABORT]
            else ""
        )
        task_name = (
            self.get_base_name(event.resource.name) if TASK_NAME_TYPE == "base" else event.resource.name
        )
        try:
            for res in event.related:
                if res["prefect.resource.id"].split(".")[1] == "flow-run":
                    flow_run_id = res["prefect.resource.id"].split(".")[-1]
            if event_state != RunState.START:
                # check cache and fetch related deployment info if not cached
                try:
                    related_deployment_info = self.related_deployment_info[flow_run_id]
                except KeyError:
                    related_deployment_info = await self.get_deployment_info(flow_run_id)
                    self.related_deployment_info[flow_run_id] = related_deployment_info
            else:
                related_deployment_info = await self.get_deployment_info(flow_run_id)
                self.related_deployment_info[flow_run_id] = related_deployment_info

            # Skip task runs without a start time
            try:
                ol_task_run_id: str = self.build_run_id(
                    start_time, task_name, self.related_deployment_info[flow_run_id].namespace
                )
            except AttributeError:
                logger.info(
                    "No start time found for task run %s. An event will not be emitted.",
                    prefect_task_run_id,
                )
                return

            # Get datasets from Prefect Artifacts
            input_datasets = []
            output_datasets = []
            datasets = await self.get_artifacts_by_task_run(prefect_task_run_id)
            input_datasets = [
                dataset for dataset in datasets if (datasets and dataset["dataset_type"] == "input")
            ]
            output_datasets = [
                dataset for dataset in datasets if (datasets and dataset["dataset_type"] == "output")
            ]

            # Get job dependencies (Prefect "parents") info for JobDependenciesRunFacet
            parent_runs = await self.get_parent_runs(event.payload, prefect_task_run_id)

            # Get flow run info for ParentRunFacet
            flow_info = await self.get_flow_info(self.related_deployment_info[flow_run_id].flow_run)

            ol_flow_run_id = ""
            try:
                ol_flow_run_id = self.build_run_id(
                    flow_info.start_time, flow_info.name, self.related_deployment_info[flow_run_id].namespace
                )
            except AttributeError:
                logger.info(
                    "Failed to build OpenLineage run id for Flow %s. \
                    ParentRunFacet will not be included.",
                    self.related_deployment_info[flow_run_id].flow_run,
                )
            task_info = TaskInfo(
                run_id=ol_task_run_id,
                event_time=event_time,
                name=task_name,
                namespace=self.related_deployment_info[flow_run_id].namespace,
                event_type=event_state,
                expected_start_time=expected_start_time,
                job_deps=parent_runs,
                prefect_version=prefect_version,
                state_msg=prefect_state_msg,
            )
            datasets = DatasetInfo(
                input_datasets=input_datasets,
                output_datasets=output_datasets,
            )

            self.ol_adapter.create_and_emit_task_event(
                task_info=task_info,
                flow_run_id=ol_flow_run_id,
                flow_name=flow_info.name,
                deployment=self.related_deployment_info[flow_run_id],
                datasets=datasets,
            )

        except (PrefectHTTPStatusError, ObjectNotFound):
            logger.info(
                "No Prefect run found for %s, will not attempt to create OpenLineage event.",
                prefect_task_run_id,
            )

    async def collect_and_process_runs(self) -> None:
        """Collects and processes Prefect events."""

        async with get_events_subscriber(filter=self.filter_criteria) as subscriber, self.client:
            prefect_version = await self.get_prefect_version()

            async for event in subscriber:
                try:
                    entity_type = event.event.split(".")[1]
                    prefect_state = event.event.split(".")[-1]

                    if prefect_state not in ["Running", "Completed", "Cancelled", "Failed", "Crashed"]:
                        continue
                    match prefect_state:
                        case "Running":
                            event_state = RunState.START
                        case "Completed":
                            event_state = RunState.COMPLETE
                        case "Cancelled":
                            event_state = RunState.ABORT
                        case "Failed" | "Crashed":
                            event_state = RunState.FAIL

                    if entity_type == "flow-run":
                        await self.collect_and_process_flow_runs(prefect_version, event, event_state)

                    if entity_type == "task-run":
                        await self.collect_and_process_task_runs(prefect_version, event, event_state)
                except Exception:
                    logger.exception("Error processing Prefect event %s", event.event)


async def main():
    logger.info("Starting PrefectOpenLineageListener...")
    await PrefectOpenLineageListener().collect_and_process_runs()


if __name__ == "__main__":
    asyncio.run(main())
