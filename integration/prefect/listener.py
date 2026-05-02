# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import asyncio

from prefect.events.clients import get_events_subscriber
from prefect.client.orchestration import get_client
from prefect.events.filters import EventFilter, EventNameFilter
from prefect.runtime import task_run
from . import adapter, PrefectOpenLineageAdapter

async def stream_task_runs():
	"""Requires that PREFECT_API_URL environment variable be set"""

	async def get_flow_name_from_task_id(task_run_id: str):

		async with get_client() as client:
			task_run = await client.read_task_run(task_run_id)
			flow_run_id = task_run.flow_run_id
			flow_run = await client.read_flow_run(flow_run_id)
			flow_id = flow_run.flow_id
			flow = await client.read_flow(flow_id)
			return flow.name

	filter_criteria = EventFilter(
    	event=EventNameFilter(prefix=["prefect.task-run."])
	)

	async with get_events_subscriber(filter=filter_criteria) as subscriber:
        
		async for event in subscriber:
			event_time = event.occurred

			task_name = event.resource.name.split("-")[0]

			if 'Running' in event.event:
				event_type = 'Running'
				task_run_id = event.resource.id.split(".")[-1]
				flow_name = await get_flow_name_from_task_id(task_run_id)

				ol_adapter = PrefectOpenLineageAdapter()
				ol_adapter.create_and_emit_events(event_type, event_time, flow_name, task_name)

			elif 'Completed' in event.event:
				event_type = 'Complete'
				task_run_id = event.resource.id.split(".")[-1]
				flow_name = await get_flow_name_from_task_id(task_run_id)

				ol_adapter = PrefectOpenLineageAdapter()
				ol_adapter.create_and_emit_events(event_type, event_time, flow_name, task_name)

			elif 'Failed' in event.event:
				event_type = 'Failed'
				task_run_id = event.resource.id.split(".")[-1]
				flow_name = await get_flow_name_from_task_id(task_run_id)

				ol_adapter = PrefectOpenLineageAdapter()
				ol_adapter.create_and_emit_events(event_type, event_time, flow_name, task_name)

asyncio.run(stream_task_runs())
