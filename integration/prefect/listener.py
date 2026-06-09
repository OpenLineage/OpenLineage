# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import ast
import asyncio
from datetime import datetime
import logging
import os
import re
import requests
from uuid import UUID

from adapter import PrefectOpenLineageAdapter
from openlineage.client.uuid import generate_static_uuid, generate_new_uuid
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import ArtifactFilter, ArtifactFilterTaskRunId
from prefect.events.clients import get_events_subscriber
from prefect.events.filters import EventFilter, EventNameFilter
from prefect.events.schemas.events import Event
from prefect.runtime import task_run, flow_run
from prefect.utilities.urls import url_for

JOB_NAMESPACE: str = os.environ.get("OPENLINEAGE_NAMESPACE", "prefect_test") #TODO: make "default" for prod

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

	async def get_deployment_info(self, flow_run_id: str) -> dict:
		flow_run = await self.client.read_flow_run(flow_run_id) # TODO: type
		deployment = await self.client.read_deployment(flow_run.deployment_id)
		return {
			"id": str(deployment.id), 
			"created": deployment.created.isoformat(), 
			"updated": deployment.updated.isoformat(), 
			"name": deployment.name
		}

	async def get_prefect_version(self, client) -> str | None:
		try:
			response = await client._client.get("/admin/version")
			version = response.json()
			return version
		except TypeError:
			logger.info("Cannot get Prefect version. PREFECT_API_URL not set.")

	async def get_flow_ns(self, flow_run_id: str) -> str:
		"""
		Looks for OPENLINEAGE_NAMESPACE job env variable in deployment.
		"""
		flow_run = await self.client.read_flow_run(flow_run_id) # TODO: type
		deployment = await self.client.read_deployment(flow_run.deployment_id)
		try:
			ns: str = deployment.job_variables["env"]["OPENLINEAGE_NAMESPACE"]
		except KeyError:
			ns: str = JOB_NAMESPACE
			logger.info(
				"OPENLINEAGE_NAMESPACE deployment variable not found. Using OPENLINEAGE_NAMESPACE env variable."
			)
			if JOB_NAMESPACE == "default":
				logger.info("OPENLINEAGE_NAMESPACE env variable not set. Namespace will be 'default.'")
		return ns

	async def get_job_ns(self, task_run_id: str) -> str:
		"""
		Looks for OPENLINEAGE_NAMESPACE job env variable in parent deployment.
		"""
		task_run = await self.client.read_task_run(task_run_id) # TODO: type
		return await self.get_flow_ns(task_run.flow_run_id)

	async def get_flow_and_deployment_info(self, flow_run_id: str) -> dict:
		flow_run = await self.client.read_flow_run(flow_run_id) # TODO: type
		flow_id: UUID = flow_run.flow_id
		flow = await self.client.read_flow(flow_id) # TODO: type
		flow_name: str = flow.name
		deployment_info: dict = await self.get_deployment_info(flow_run_id)
		return {"name": flow_name, "deployment_info": deployment_info}

	async def get_flow_run_start_time(self, flow_run_id: str) -> datetime:
		flow_run = await self.client.read_flow_run(flow_run_id) # TODO: type
		flow_run_start_time = flow_run.start_time
		return flow_run_start_time

	async def get_artifacts_by_task_run(self, run_id: str, client) -> list:
		payload = {
			"artifacts": {
				"task_run_id": {
					"any_": [run_id]
				}
			}
		}
		response = await client._client.post("/artifacts/filter", json=payload)
		if response.status_code == 200:
			dataset_info = []
			artifacts = response.json()
			for artifact in artifacts:
				if "ol-dataset" in artifact["description"]:
					dataset_type = artifact["description"].split("_")[-1]
					data_list = ast.literal_eval(artifact["data"])
					uri = data_list[0]["database_uri"]
					table = data_list[0]["table"]
					dataset_info.append({"uri": uri, "table": table, "dataset_type": dataset_type})
			return dataset_info
		else:
			logging.info("No datasets found for task run.")
			return []

	async def get_parent_runs(self, payload: dict, prefect_task_run_id: str) -> list:
		try:
			parent_runs = []
			task_parents: list = payload["task_run"]["task_inputs"]["__parents__"]
			for parent in task_parents:
				task_run_id: str | None = parent["id"] if parent["input_type"] == "task_run" else None
				if task_run_id:
					parent_namespace: dict = await self.get_job_ns(task_run_id)
					parent_run = await self.client.read_task_run(task_run_id)
					parent_name: str = parent_run.name.split("-")[0]
					parent_run_id: str = self.build_run_id(
										parent_run.start_time,
										parent_name, 
										parent_namespace
									)
					parent_runs.append({
									"name": parent_name, 
									"namespace": parent_namespace, 
									"id": parent_run_id
								})
			return parent_runs
		except KeyError:
			logger.info("No task parents found for %s", prefect_task_run_id)
			return []

	async def collect_and_process_flow_runs(self, prefect_version: str, event: Event, event_state: str):
		for res in event.related:
			if res["prefect.resource.role"] == "flow":
				flow_run_name: str = event.resource.name
				flow_name: str = res["prefect.resource.name"]

			if flow_run_name:
				prefect_flow_run_id: str = event.resource.id.split(".")[-1]
				event_time: datetime = datetime.fromisoformat(event.resource["prefect.state-timestamp"])
				start_time: datetime = await self.get_flow_run_start_time(prefect_flow_run_id)
				flow_namespace: str = await self.get_flow_ns(prefect_flow_run_id)
				flow_deployment_info: dict = await self.get_deployment_info(prefect_flow_run_id)
				deployment_id: str = flow_deployment_info["id"]
				deployment_created: str = flow_deployment_info["created"]
				deployment_updated: str = flow_deployment_info["updated"]
				deployment_name: str = flow_deployment_info["name"]
				ol_flow_run_id: str = self.build_run_id(
					start_time,
					flow_name,
					flow_namespace
				)

				self.ol_adapter.create_and_emit_flow_event(
					runId=ol_flow_run_id,
					eventType=event_state, 
					eventTime=event_time,
					flowName=flow_name,
					flowNamespace=flow_namespace,
					prefectVersion=prefect_version,
					deploymentId=deployment_id,
					deploymentCreated=deployment_created,
					deploymentUpdated=deployment_updated,
					deploymentName=deployment_name
				)

	async def collect_and_process_task_runs(self, prefect_version: str, event: Event, event_state: str, client: client):
		task_name: str = event.resource.name.split("-")[0]
		task_run_name: str = event.resource.name
		event_time: datetime = datetime.fromisoformat(event.resource["prefect.state-timestamp"])
		expected_start_time: datetime = event.payload["task_run"]["expected_start_time"]
		prefect_task_run_id: str = event.resource.id.split(".")[-1]
		task_run = await self.client.read_task_run(prefect_task_run_id)
		namespace: str = await self.get_job_ns(prefect_task_run_id)
		ol_task_run_id: str = self.build_run_id(
			task_run.start_time, 
			task_name, 
			namespace
		)

		# Get datasets from Prefect Artifacts
		datasets = await self.get_artifacts_by_task_run(prefect_task_run_id, client)
		input_datasets = [dataset for dataset in datasets if dataset["dataset_type"] == "input"]
		output_datasets = [dataset for dataset in datasets if dataset["dataset_type"] == "output"]

		# Get job dependencies (Prefect "parents") info for JobDependenciesRunFacet
		parent_runs = await self.get_parent_runs(event.payload, prefect_task_run_id)

		# Get flow run info for ParentRunFacet
		for res in event.related:
			if res["prefect.resource.role"] == "flow-run":
				flow_run_id: str = res["prefect.resource.id"].split(".")[-1]
				flow_and_deployment_info: str = await self.get_flow_and_deployment_info(flow_run_id)
				flow_name: str = flow_and_deployment_info["name"]
				deployment_info: dict = flow_and_deployment_info["deployment_info"]
				deployment_id: str = deployment_info["id"]
				deployment_created: str = deployment_info["created"]
				deployment_updated: str = deployment_info["updated"]
				deployment_name: str = deployment_info["name"]
				flow_start_time = await self.get_flow_run_start_time(flow_run_id)
				ol_flow_run_id: str = self.build_run_id(
					flow_start_time,
					flow_name,
					namespace
				)

		self.ol_adapter.create_and_emit_task_event(
			runId=ol_task_run_id,
			eventType=event_state, 
			eventTime=event_time,
			expectedEventTime=expected_start_time,
			flowRunId=ol_flow_run_id,
			flowName=flow_name,
			taskName=task_name,
			namespace=namespace,
			jobDeps=parent_runs,
			prefectVersion=prefect_version,
			deploymentId=deployment_id,
			deploymentCreated=deployment_created,
			deploymentUpdated=deployment_updated,
			deploymentName=deployment_name,
			inputDatasets=input_datasets,
			outputDatasets=output_datasets
		)

	async def collect_and_process_runs(self) -> None:
		try:
			os.environ.get("PREFECT_API_URL")
		except TypeError:
			logger.warn("PREFECT_API_URL not set. Prefect events will not be emitted.")
			return

		filter_criteria = EventFilter(
	    	event = EventNameFilter(prefix=["prefect.task-run.", "prefect.flow-run.", "prefect.asset.materialization."])
		)

		async with get_events_subscriber(filter=filter_criteria) as subscriber:
			async with self.client:
				prefect_version: str = await self.get_prefect_version(self.client)

				async for event in subscriber:
					entity_type: str = event.event.split(".")[1]
					prefect_state: str = event.event.split(".")[-1]

					if prefect_state in ["Running", "Completed", "Failed"]:

						match prefect_state:
							case 'Running':
								event_state: str = "START"
							case 'Completed':
								event_state: str = "COMPLETE"
							case 'Failed':
								event_state: str = "FAILED"

						if entity_type == "flow-run":
							await self.collect_and_process_flow_runs(prefect_version, event, event_state)

						if entity_type == "task-run":
							await self.collect_and_process_task_runs(prefect_version, event, event_state, self.client)

asyncio.run(PrefectOpenLineageListener().collect_and_process_runs())
