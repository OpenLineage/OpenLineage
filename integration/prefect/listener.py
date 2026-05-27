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
from openlineage.client.uuid import generate_static_uuid, generate_new_uuid
from prefect.events.clients import get_events_subscriber
from prefect.client.orchestration import get_client
from prefect.events.filters import EventFilter, EventNameFilter
from prefect.runtime import task_run, flow_run
from prefect.utilities.urls import url_for

JOB_NAMESPACE: str = os.environ.get("OPENLINEAGE_NAMESPACE", "prefect_test")

logger: logging.Logger = logging.getLogger(__name__)

class PrefectOpenLineageListener:
	def __init__(
		self,
		client = None,
		adapter = None,
	):
		self.client = client or get_client()
		self.ol_adapter = adapter or PrefectOpenLineageAdapter()

	def build_run_id(
		self,
		execution_time: datetime, 
		run_name: str, 
		namespace: str
	) -> str:
		return str(generate_static_uuid(
			instant=execution_time,
	        data=f"{namespace}.{run_name}".encode("utf-8"),
		))

	async def get_deployment_info(self, flow_run_id: str):
		flow_run = await self.client.read_flow_run(flow_run_id) # TODO: type
		deployment = await self.client.read_deployment(flow_run.deployment_id)
		return {
			"id": str(deployment.id), 
			"created": deployment.created.isoformat(), 
			"updated": deployment.updated.isoformat(), 
			"name": deployment.name
		}

	def get_prefect_version(self):
		"""Requires PREFECT_API_URL."""
		url = os.environ.get("PREFECT_API_URL")+"/admin/version"
		version = requests.get(url).json()
		return version

	async def get_task_run(self, task_id: str):
		task_run = await self.client.read_task_run(task_id) # TODO: type
		return task_run

	async def get_flow_ns(self, flow_run_id: str):
		"""
		Looks for OPENLINEAGE_NAMESPACE job env variable in flow's deployment.
		"""
		flow_run = await self.client.read_flow_run(flow_run_id) # TODO: type
		deployment = await self.client.read_deployment(flow_run.deployment_id)
		try:
			ns: str = deployment.job_variables["env"]["OPENLINEAGE_NAMESPACE"]
		except:
			ns: str = JOB_NAMESPACE
		return ns

	async def get_job_ns(self, task_run_id: str):
		"""
		Looks for OPENLINEAGE_NAMESPACE job env variable in task's deployment.
		"""
		task_run = await self.client.read_task_run(task_run_id) # TODO: type
		return await self.get_flow_ns(task_run.flow_run_id)

	async def get_flow_and_deployment_info(self, task_run_id: str) -> dict:
		task_run = await self.client.read_task_run(task_run_id) # TODO: type
		flow_run_id: UUID = task_run.flow_run_id
		flow_run = await self.client.read_flow_run(flow_run_id) # TODO: type
		flow_id: UUID = flow_run.flow_id
		deployment_info: dict = await self.get_deployment_info(flow_run_id)
		flow = await self.client.read_flow(flow_id) # TODO: type
		flow_name: str = flow.name
		return {"name": flow_name, "deployment_info": deployment_info}

	async def get_flow_run_start_time(self, flow_run_id: str):
		flow_run = await self.client.read_flow_run(flow_run_id) # TODO: type
		flow_run_start_time = flow_run.state.timestamp
		return flow_run_start_time

	async def collect_and_process_task_runs(self):
		"""Requires PREFECT_API_URL."""
		filter_criteria = EventFilter(
	    	event = EventNameFilter(prefix=["prefect.task-run.", "prefect.flow-run."])
		)
		run_context: dict = {}
		prefect_version = self.get_prefect_version()

		async with get_events_subscriber(filter=filter_criteria) as subscriber:
			async with self.client:
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

							for res in event.related:
								if res["prefect.resource.role"] == "flow":
									flow_run_name: str = event.resource.name
									flow_name: str = res["prefect.resource.name"]

							if flow_run_name:
								start_time: datetime = datetime.fromisoformat(event.resource["prefect.state-timestamp"])
								prefect_flow_run_id: str = event.resource.id.split(".")[-1]
								flow_namespace: str = await self.get_flow_ns(prefect_flow_run_id)
								flow_deployment_info: dict = await self.get_deployment_info(prefect_flow_run_id)

								# Using caching for run IDs for now because flow event lacks start_time
								try:
									ol_flow_run_id: str = run_context[flow_run_name]
								except KeyError:
									ol_flow_run_id: str = self.build_run_id(
										start_time,
										flow_name,
										flow_namespace
									)
									run_context[flow_run_name] = ol_flow_run_id

								self.ol_adapter.create_and_emit_flow_event(
									runId=ol_flow_run_id,
									eventType=event_type, 
									eventTime=start_time,
									flowName=flow_name,
									flowNamespace=flow_namespace,
									prefectVersion=prefect_version,
									flowDeploymentInfo=flow_deployment_info
								)

						elif entity_type == "task-run":

							task_name: str = event.resource.name.split("-")[0]
							task_run_name: str = event.resource.name
							start_time: datetime = datetime.fromisoformat(event.payload["task_run"]["start_time"])
							event_time: datetime = datetime.fromisoformat(event.resource["prefect.state-timestamp"])
							expected_start_time: datetime = event.payload["task_run"]["expected_start_time"]
							prefect_task_run_id: str = event.resource.id.split(".")[-1]
							namespace: str = await self.get_job_ns(prefect_task_run_id)

							ol_task_run_id: str = self.build_run_id(
								start_time, 
								task_name, 
								namespace
							)

							# Get job dependencies (Prefect "parents") info for JobDependenciesRunFacet
							parent_runs = []
							try:
								task_parents: List = event.payload["task_run"]["task_inputs"]["__parents__"]
								for parent in task_parents:
									task_run_id: str | None = parent["id"] if parent["input_type"] == "task_run" else None

									if task_run_id:
										parent_namespace: dict = await self.get_job_ns(task_run_id)
										parent_run = await self.get_task_run(task_run_id)
										parent_name: str = parent_run.name.split("-")[0]
										parent_run_id = self.build_run_id(
															start_time, #TODO: get start_time from parent run
															parent_name, 
															parent_namespace
														)
										parent_runs.append({
														"name": parent_name, 
														"namespace": parent_namespace, 
														"id": parent_run_id
													})
							except KeyError:
								logger.info("No task parents found for %s", prefect_task_run_id)
								pass

							# Get flow run info for ParentRunFacet
							for res in event.related:
								if res["prefect.resource.role"] == "flow-run":
									flow_run_name: str = res["prefect.resource.name"]
							flow_and_deployment_info: str = await self.get_flow_and_deployment_info(prefect_task_run_id) #TODO: use flow_run_id
							flow_name: str = flow_and_deployment_info["name"]
							deployment_info: dict = flow_and_deployment_info["deployment_info"]
							ol_flow_run_id: str = run_context[flow_run_name]

							self.ol_adapter.create_and_emit_task_event(
								runId=ol_task_run_id,
								eventType=event_type, 
								eventTime=event_time,
								expectedEventTime=expected_start_time,
								flowRunId=ol_flow_run_id,
								flowName=flow_name,
								taskName=task_name,
								namespace=namespace,
								jobDeps=parent_runs,
								prefectVersion=prefect_version,
								flowDeploymentInfo=deployment_info
							)

asyncio.run(PrefectOpenLineageListener().collect_and_process_task_runs())
