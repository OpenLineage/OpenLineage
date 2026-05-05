# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Advisory: This integration is experimental and in active development.

import asyncio
from datetime import datetime
from uuid import UUID

from prefect.events.clients import get_events_subscriber
from prefect.client.orchestration import get_client
from prefect.events.filters import EventFilter, EventNameFilter
from prefect.runtime import task_run
from adapter import PrefectOpenLineageAdapter

async def get_flow_name_from_task_id(task_run_id: str):

	async with get_client() as client:

		task_run = await client.read_task_run(task_run_id) # to do
		flow_run_id: UUID = task_run.flow_run_id
		flow_run = await client.read_flow_run(flow_run_id) # to do
		flow_id: UUID = flow_run.flow_id
		flow = await client.read_flow(flow_id) # to do

		return flow.name

async def stream_task_runs():
	"""Requires that PREFECT_API_URL environment variable be set"""	

	filter_criteria = EventFilter(
    	event=EventNameFilter(prefix=["prefect.task-run."])
	)

	async with get_events_subscriber(filter=filter_criteria) as subscriber:
        
		async for event in subscriber:

			event_time: datetime = event.occurred
			task_name: str = event.resource.name.split("-")[0]
			event_state: str = event.event.split(".")[-1]

			if event_state in ['Running', 'Completed', 'Failed']:

				match event_state:
					case 'Running':
						event_type: str = 'RUNNING'
					case 'Completed':
						event_type: str = 'COMPLETE'
					case 'Failed':
						event_type: str = 'FAILED'

				task_run_id = event.resource.id.split(".")[-1]
				flow_name = await get_flow_name_from_task_id(task_run_id)

				ol_adapter = PrefectOpenLineageAdapter()
				ol_adapter.create_and_emit_events(event_type, event_time, flow_name, task_name)

asyncio.run(stream_task_runs())
