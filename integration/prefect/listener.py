# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import asyncio
from datetime import datetime
import logging
import os
from typing import List
from uuid import UUID

from adapter import PrefectOpenLineageAdapter
from openlineage.client.uuid import generate_static_uuid
from prefect.events.clients import get_events_subscriber
from prefect.client.orchestration import get_client
from prefect.events.filters import EventFilter, EventNameFilter
from prefect.runtime import task_run

FLOW_NAMESPACE: str = os.environ.get('FLOW_NAMESPACE', 'flow_test')
JOB_NAMESPACE: str = os.environ.get('OPENLINEAGE_NAMESPACE', 'prefect_test')
PARENT_RUN_NAMESPACE: str = os.environ.get('PARENT_RUN_NAMESPACE', 'parent_test')
OL_ADAPTER = PrefectOpenLineageAdapter()

logger: logging.Logger = logging.getLogger(__name__)

@staticmethod
def build_run_id(
	execution_time: datetime, 
	run_name: str, 
	namespace: str
) -> str:
	return str(generate_static_uuid(
		instant=execution_time,
        data=f"namespace.{run_name}".encode("utf-8"),
	))

async def get_task_run_from_task_id(task_id: str):

	async with get_client() as client:
		task_run = await client.read_task_run(task_id) # TODO: type
		return task_run

async def get_flow_run_from_task_id(task_run_id: str):

	async with get_client() as client:
		task_run = await client.read_task_run(task_run_id) # TODO: type
		flow_run_id: UUID = task_run.flow_run_id
		flow_run = await client.read_flow_run(flow_run_id) # TODO: type
		flow_id: UUID = flow_run.flow_id
		flow = await client.read_flow(flow_id) # TODO: type
		return {"start_time": flow_run.start_time, "name": flow.name}

async def collect_and_process_task_runs():
	"""Requires PREFECT_API_URL environment variable be set"""

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
				ol_task_run_id: str = build_run_id(event_time, task_name, JOB_NAMESPACE)

				# Get flow run info
				flow_namespace: str = FLOW_NAMESPACE
				flow_data = await get_flow_run_from_task_id(prefect_task_run_id)
				flow_name = flow_data["name"]
				flow_run_id: str = build_run_id(flow_data["start_time"], flow_name, FLOW_NAMESPACE)

				# Get job dependencies (Prefect "parents") info
				parents: bool = False
				try:
					parent_runs: list = []
					task_parents: List = event.payload["task_run"]["task_inputs"]["__parents__"]
					for parent in task_parents:
						task_id: str | None = parent["id"] if parent["input_type"] == "task_run" else None
						if task_id:
							parent_run = await get_task_run_from_task_id(task_id)
							parent_name = parent_run.name.split("-")[0]
							parent_run_id = build_run_id(parent_run.start_time, parent_name, PARENT_RUN_NAMESPACE)
							parent_runs.append({"name": parent_name, "namespace": PARENT_RUN_NAMESPACE, "id": parent_run_id})
					parents = True
				except:
					logger.info(f"No task parents found for {prefect_task_run_id}.")
					pass

				match event_state:
					case 'Running':
						event_type: str = "RUNNING"
					case 'Completed':
						event_type: str = "COMPLETE"
					case 'Failed':
						event_type: str = "FAILED"

				OL_ADAPTER.create_and_emit_event(
					runId=ol_task_run_id,
					eventType=event_type, 
					eventTime=event_time,
					flowRunId=flow_run_id,
					flowName=flow_name,
					flowNamespace=flow_namespace,
					taskName=task_name, 
					jobDeps=parent_runs
				)

asyncio.run(collect_and_process_task_runs())
