# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Warning: this integration is experimental and in active development.

import logging

from adapter import TemporalOpenLineageAdapter
from openlineage.client.run import RunState
from temporalio.client import Client
from temporalio.service import RPCError

logger: logging.Logger = logging.getLogger(__name__)
COMPLETE_STATUS_VALUE = 2
FAIL_STATUS_VALUE = 3

async def get_temporal_events(event_data: list, t_client: Client) -> None:
    adapter = TemporalOpenLineageAdapter()
    input_datasets = []
    output_datasets = []

    for workflow in event_data:

        try:
            description = await t_client.get_workflow_handle(workflow["id"]).describe()
        except RPCError:
            logger.info("Description not found for workflow with id %", workflow["id"])
            continue

        start_event_name = description.id
        start_event_time = description.start_time
        start_event_run_id = adapter.build_run_id(start_event_time, start_event_name)

        try:
            input_datasets = [
                dataset
                for dataset in workflow["datasets"]
                if dataset["type"] == "input"
            ]
        except KeyError:
            logger.info("No input datasets found for workflow %", workflow["id"])

        try:
            output_datasets = [
                dataset
                for dataset in workflow["datasets"]
                if dataset["type"] == "output"
            ]
        except KeyError:
            logger.info("No output datasets found for workflow %", workflow["id"])
            output_datasets = []

        # Start event
        adapter.create_and_emit_task_event(
            start_event_run_id, RunState.START, start_event_time, start_event_name
        )

        # Complete event
        if description.raw_info.status == COMPLETE_STATUS_VALUE:
            complete_event_name = description.id
            complete_event_time = description.close_time

            adapter.create_and_emit_task_event(
                start_event_run_id,
                RunState.COMPLETE,
                complete_event_time,
                complete_event_name,
                input_datasets,
                output_datasets,
            )

        # Fail event
        elif description.raw_info.status == FAIL_STATUS_VALUE:
            complete_event_name = description.id
            complete_event_time = description.close_time

            adapter.create_and_emit_task_event(
                start_event_run_id,
                RunState.FAIL,
                complete_event_time,
                complete_event_name,
            )
