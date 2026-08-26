# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from temporalio.client import Client
from temporalio.service import ( RPCError )

from adapter import TemporalOpenLineageAdapter


async def get_temporal_events(workflow_ids: list, t_client: Client) -> None:
    """Get events from the Temporal API, process them, and pass them to the adapter."""
    adapter = TemporalOpenLineageAdapter()

    temporal_events = []
    for workflow_id in workflow_ids:
        try:
            description = await t_client.get_workflow_handle(workflow_id).describe()
        except RPCError:
            print(f"Description not found for workflow with id {workflow_id}")
            continue
        start_event_name = description.id
        start_event_time = description.start_time
        start_event_run_id = adapter.build_run_id(start_event_time, start_event_name)

        adapter.create_and_emit_task_event(
                                            start_event_run_id, 
                                            RunState.START, 
                                            start_event_time, 
                                            start_event_name
                                            )

        if description.raw_info.status == 2:
            complete_event_name = description.id
            complete_event_time = description.close_time
            adapter.create_and_emit_task_event(
                                                start_event_run_id, 
                                                RunState.COMPLETE, 
                                                complete_event_time, 
                                                complete_event_name
                                                )

        elif description.raw_info.status == 3:
            complete_event_name = description.id
            complete_event_time = description.close_time
            adapter.create_and_emit_task_event(
                                                start_event_run_id, 
                                                RunState.FAIL, 
                                                complete_event_time, 
                                                complete_event_name
                                                )
