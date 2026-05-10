# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import asyncio
from datetime import datetime
import os
from typing import List
from uuid import UUID

from openlineage.client.uuid import generate_new_uuid
from prefect.events.clients import get_events_subscriber
from prefect.client.orchestration import get_client
from prefect.events.filters import EventFilter, EventNameFilter
from prefect.runtime import task_run
from adapter import PrefectOpenLineageAdapter

JOB_NAMESPACE: str = os.environ.get('OPENLINEAGE_NAMESPACE')
OL_ADAPTER = PrefectOpenLineageAdapter()

async def get_task_run_from_task_id(task_id: str):

	async with get_client() as client:
		task_run = await client.read_task_run(task_id) # to do
		return task_run

async def get_flow_name_from_task_id(task_run_id: str):

	async with get_client() as client:
		task_run = await client.read_task_run(task_run_id) # to do
		flow_run_id: UUID = task_run.flow_run_id
		flow_run = await client.read_flow_run(flow_run_id) # to do
		flow_id: UUID = flow_run.flow_id
		flow = await client.read_flow(flow_id) # to do

		return flow.name

async def collect_and_process_task_runs():
	"""Requires PREFECT_API_URL environment variable be defined"""	
	task_context: dict = {}

	filter_criteria = EventFilter(
    	event = EventNameFilter(prefix=["prefect.task-run."])
	)

	async with get_events_subscriber(filter=filter_criteria) as subscriber:
        
		async for event in subscriber:

			event_time: datetime = event.occurred
			task_name: str = event.resource.name.split("-")[0]
			event_state: str = event.event.split(".")[-1] 

			if event_state in ["Running", "Completed", "Failed"]:

				prefect_task_run_id: str = event.resource.id.split(".")[-1]
				flow_name: str = await get_flow_name_from_task_id(prefect_task_run_id)

				ol_run_id: str = str(generate_new_uuid())
				task_context[task_name] = ol_run_id

				parent_runs: list = None
				parents: bool = False
				try:
					parent_runs = []
					task_parents: List = event.payload["task_run"]["task_inputs"]["__parents__"]
					for parent in task_parents:
						task_id: str = parent["id"] if parent["input_type"] == "task_run" else None
						if task_id:
							parent_run = await get_task_run_from_task_id(task_id)
							parent_name = parent_run.name.split("-")[0]
							parent_runs.append({"name": parent_name, "namespace": "parent_namespace", "id": task_context[parent_name]})
					parents = True
				except:
					pass

				match event_state:
					case 'Running':
						event_type: str = "RUNNING"
					case 'Completed':
						event_type: str = "COMPLETE"
					case 'Failed':
						event_type: str = "FAILED"

				OL_ADAPTER.create_and_emit_event(
					ol_run_id,
					event_type, 
					event_time, 
					flow_name, 
					task_name, 
					parentRuns = parent_runs if parents else None
				)

asyncio.run(collect_and_process_task_runs())
