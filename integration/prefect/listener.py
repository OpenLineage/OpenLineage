# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import asyncio
from datetime import datetime
import logging
import os
import requests
from typing import List
from uuid import UUID

from adapter import PrefectOpenLineageAdapter
from openlineage.client.uuid import generate_static_uuid
from prefect.events.clients import get_events_subscriber
from prefect.client.orchestration import get_client
from prefect.events.filters import EventFilter, EventNameFilter
from prefect.runtime import task_run, flow_run
from prefect.utilities.urls import url_for

JOB_NAMESPACE: str = os.environ.get('OPENLINEAGE_NAMESPACE', 'prefect_test')
OL_ADAPTER = PrefectOpenLineageAdapter()
TRANSPORT:str = os.environ.get("OPENLINEAGE_TRANSPORT", "http://localhost:5000")

logger: logging.Logger = logging.getLogger(__name__)

def build_run_id(
	execution_time: datetime, 
	run_name: str, 
	namespace
) -> str:
	return str(generate_static_uuid(
		instant=execution_time,
        data=f"namespace.{run_name}".encode("utf-8"),
	))

def get_prefect_version():
	"""Requires PREFECT_API_URL"""

	url = os.environ.get("PREFECT_API_URL")+"/admin/version"
	version = requests.get(url).json()
	return version

def get_variable(url: str, var: str):

	url = url+f"/variables/{var}"
	prefect_variable = requests.get(url).json()
	return prefect_variable

async def get_task_run(task_id: str):

	async with get_client() as client:
		task_run = await client.read_task_run(task_id) # TODO: type
		return task_run

async def get_deployment_id(flow_run_id: str):

	async with get_client() as client:
		flow_run = await client.read_flow_run(flow_run_id) # TODO: type
		deployment = await client.read_deployment(flow_run.deployment_id)
		return str(deployment.id).split("-")[0]

async def get_job_vars(task_run_id):
	"""
	Looks for OL_NAMESPACE job env variable in parent's deployment. 
	If not found, returns deployment id.
	"""

	async with get_client() as client:
		task_run = await client.read_task_run(task_run_id) # TODO: type
		flow_run_id: UUID = task_run.flow_run_id
		flow_run = await client.read_flow_run(flow_run_id) # TODO: type
		deployment = await client.read_deployment(flow_run.deployment_id)
		try:
			ns: str = deployment.job_variables["env"]["OPENLINEAGE_NAMESPACE"]
			transport: str = deployment.job_variables["env"]["OPENLINEAGE_TRANSPORT"]
		except:
			ns: str = str(deployment.id).split("-")[0] # Was deployment.name
			transport: str = "http" # TODO
		return {"namespace": ns, "transport": transport}

async def get_flow_run_info_from_task_id(task_run_id: str):

	async with get_client() as client:
		task_run = await client.read_task_run(task_run_id) # TODO: type
		flow_run_id: UUID = task_run.flow_run_id
		flow_run = await client.read_flow_run(flow_run_id) # TODO: type
		transport = TRANSPORT.removeprefix("http://")[:-5] # deployment = await client.read_deployment(flow_run.deployment_id)
		flow_id: UUID = flow_run.flow_id
		flow = await client.read_flow(flow_id) # TODO: type
		return {
			"start_time": flow_run.start_time, 
			"flow_name": flow.name, 
			"transport": transport # "deployment_id": str(deployment.id).split("-")[0] # Was deployment.name
		}

async def collect_and_process_task_runs():
	"""Requires PREFECT_API_URL"""

	filter_criteria = EventFilter(
    	event = EventNameFilter(prefix=["prefect.task-run.", "prefect.flow-run."])
	)

	async with get_events_subscriber(filter=filter_criteria) as subscriber:

		async for event in subscriber:

			entity_type: str = event.event.split(".")[1]
			event_state: str = event.event.split(".")[-1]

			if event_state in ["Running", "Completed", "Failed"]:

				match event_state:
					case 'Running':
						event_type: str = "START"
					case 'Completed':
						event_type: str = "COMPLETE"
					case 'Failed':
						event_type: str = "FAILED"

				if entity_type == "flow-run":

					for rel in event.related:
						if rel["prefect.resource.role"] == "flow":
							flow_name: str = rel["prefect.resource.name"]
					if flow_name:
						start_time: datetime = datetime.fromisoformat(event.resource["prefect.state-timestamp"])
						flow_namespace: str = JOB_NAMESPACE
						flow_run_id: str = build_run_id(
							start_time,
							flow_name,
							flow_namespace
						)
						deployment_id: str = await get_deployment_id(event.resource.id.split(".")[-1]) # TODO: host instead?
						transport_url_long: str = TRANSPORT.split("/")[2]
						transport_url_short: str = transport_url_long[:-5]

						# Get Prefect version
						prefect_version = get_prefect_version()

						OL_ADAPTER.create_and_emit_flow_event(
							runId=flow_run_id,
							eventType=event_type, 
							eventTime=start_time,
							flowName=flow_name,
							parentTransport=transport_url_short,
							prefectVersion=prefect_version
						)

				elif entity_type == "task-run":

					task_name: str = event.resource.name.split("-")[0]
					task_run: str = event.payload["task_run"]
					start_time: datetime = datetime.fromisoformat(task_run["start_time"])
					prefect_task_run_id: str = event.resource.id.split(".")[-1]
					ol_task_run_id: str = build_run_id(start_time, task_name, JOB_NAMESPACE)

					# Get job dependencies (Prefect "parents") info
					parent_runs = []
					try:
						task_parents: List = event.payload["task_run"]["task_inputs"]["__parents__"]
						for parent in task_parents:
							task_run_id: str | None = parent["id"] if parent["input_type"] == "task_run" else None

							if task_run_id:
								job_vars: dict = await get_job_vars(task_run_id)
								parent_namespace: str = job_vars["namespace"]
								parent_transport: str = job_vars["transport"]
								parent_run = await get_task_run(task_run_id)
								parent_name: str = parent_run.name.split("-")[0]
								parent_run_id = build_run_id(
													start_time, 
													parent_name, 
													parent_transport # was parent_namespace
												)
								parent_runs.append({
												"name": parent_name, 
												"namespace": parent_namespace, 
												"id": parent_run_id
											})
					except:
						logger.info("No task parents found for %s", prefect_task_run_id)
						pass

					# Get flow run info
					flow_data = await get_flow_run_info_from_task_id(prefect_task_run_id)
					flow_name = flow_data["transport"]+"."+flow_data["flow_name"] # transport was deployment_id
					flow_run_id: str = build_run_id(flow_data["start_time"], flow_name, JOB_NAMESPACE)

					# Get Prefect version
					prefect_version = get_prefect_version()

					OL_ADAPTER.create_and_emit_task_event(
						runId=ol_task_run_id,
						eventType=event_type, 
						eventTime=start_time,
						flowRunId=flow_run_id,
						flowName=flow_name,
						flowNamespace=JOB_NAMESPACE,
						taskName=task_name, 
						jobDeps=parent_runs,
						prefectVersion=prefect_version
					)

asyncio.run(collect_and_process_task_runs())
