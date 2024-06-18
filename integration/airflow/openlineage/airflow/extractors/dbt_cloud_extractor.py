# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import asyncio
import re
import traceback

# for concurrent processing
from contextlib import suppress
from typing import Dict, List, Optional

import aiohttp
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.facet_v2 import error_message_run
from openlineage.common.provider.dbt import DbtCloudArtifactProcessor, ParentRunMetadata

from airflow.hooks.base import BaseHook
from airflow.providers.dbt.cloud.hooks.dbt import (
    DbtCloudHook,
    fallback_to_default_account,
)


class DbtCloudExtractor(BaseExtractor):
    default_schema = "public"

    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return [
            "DbtCloudRunJobOperator",
            "DbtCloudJobRunSensor",
            "DbtCloudRunJobOperatorAsync",
            "DbtCloudJobRunSensorAsync",
        ]

    def get_task_metadata(self):
        try:
            operator = self.operator
            task_name = f"{operator.dag_id}.{operator.task_id}"
            run_facets: Dict = {}
            job_facets: Dict = {}

        except Exception as e:
            self.log.exception("Exception has occurred in extract()")
            error = error_message_run.ErrorMessageRunFacet(str(e), "python")
            error.stackTrace = traceback.format_exc()
            run_facets["errorMessage"] = error
            raise Exception(e)
        finally:
            # finally return the task metadata
            return TaskMetadata(
                name=task_name,
                inputs=[],
                outputs=[],
                run_facets=run_facets,
                job_facets=job_facets,
            )

    def extract(self) -> TaskMetadata:
        operator = self.operator
        # according to the job type, pre-perform the necessary
        # fetch (job and project) of data so that it will save
        # time when creating lineage data at the completion
        if operator.task_type in [
            "DbtCloudRunJobOperator",
            "DbtCloudRunJobOperatorAsync",
        ]:
            job_id = operator.job_id
            hook = DbtCloudHook(operator.dbt_cloud_conn_id)
            # use job_id to get connection
            job = hook.get_job(job_id=job_id).json()["data"]
            project_id = job["project_id"]
            project = hook.get_project(project_id=project_id).json()["data"]
            connection = project["connection"]
            self.context["connection"] = connection
            self.context["job"] = job

        elif operator.task_type in [
            "DbtCloudJobRunSensor",
            "DbtCloudJobRunSensorAsync",
        ]:
            run_id = operator.run_id
            connection = BaseHook.get_connection(operator.dbt_cloud_conn_id)
            account_id = connection.login
            hook = DbtCloudHook(operator.dbt_cloud_conn_id)
            job_run = hook.get_job_run(account_id=account_id, run_id=run_id, include_related=["job"]).json()[
                "data"
            ]
            project_id = job_run["project_id"]
            job = job_run["job"]
            project = hook.get_project(project_id=project_id, account_id=account_id).json()["data"]
            connection = project["connection"]
            self.context["connection"] = connection
            self.context["job"] = job

        return self.get_task_metadata()

    # internal method to extract dbt lineage and send it to
    # OL backend
    def extract_dbt_lineage(self, operator, run_id, job_name=None):
        from openlineage.airflow.adapter import (
            _DAG_NAMESPACE,
            _PRODUCER,
            OpenLineageAdapter,
        )

        job = self.context["job"]
        account_id = job["account_id"]
        hook = DbtCloudHook(operator.dbt_cloud_conn_id)
        execute_steps = job["execute_steps"]
        job_run = hook.get_job_run(
            run_id=run_id, account_id=account_id, include_related=["run_steps"]
        ).json()["data"]
        run_steps = job_run["run_steps"]
        connection = self.context["connection"]
        steps = []

        for run_step in run_steps:
            name = run_step["name"]
            if name.startswith("Invoke dbt with `"):
                regex_pattern = "Invoke dbt with `([^`.]*)`"
                m = re.search(regex_pattern, name)
                command = m.group(1)
                if command in execute_steps:
                    steps.append(run_step["index"])

        headers, tenant = DbtCloudExtractor.get_headers_tenants_from_connection(hook)

        catalog = None
        with suppress(Exception):
            catalog = hook.get_job_run_artifact(run_id, path="catalog.json").json()["data"]

        async def get_steps(steps):
            tasks = [
                self.get_dbt_artifacts(
                    hook,
                    run_id,
                    account_id,
                    step,
                    ["manifest", "run_results"],
                    headers,
                    tenant,
                )
                for step in steps
            ]
            return await asyncio.gather(*tasks)

        loop = asyncio.new_event_loop()
        step_artifacts = loop.run_until_complete(get_steps(steps))
        for artifacts in step_artifacts:
            # process manifest
            manifest = artifacts["manifest"]
            run_reason = manifest["metadata"]["env"]["DBT_CLOUD_RUN_REASON"]
            # ex: Triggered via Apache Airflow by task 'trigger_job_run1' in the astronomy DAG.
            # regex pattern:
            #   Triggered via Apache Airflow by task '[a-zA-Z0-9_]+' in the [a-zA-Z-0-9_]+ DAG.
            regex_pattern = (
                "Triggered via Apache Airflow by task '([a-zA-Z0-9_]+)' " "in the ([a-zA-Z0-9_]+) DAG\\."
            )
            m = re.search(regex_pattern, run_reason)
            task_id = m.group(1)
            dag_id = m.group(2)

            if not artifacts.get("run_results", None):
                continue

            processor = DbtCloudArtifactProcessor(
                producer=_PRODUCER,
                job_namespace=_DAG_NAMESPACE,
                skip_errors=False,
                logger=self.log,
                manifest=manifest,
                run_result=artifacts["run_results"],
                profile=connection,
                catalog=catalog,
            )

            # parent run is an Airflow task triggering DBT Cloud run
            parent_run_id = self.context["task_uuid"]

            # if parent run id exists, set it as parent run metadata - so that
            # the DBT job can contain parent information (which is the current task)
            if parent_run_id is not None:
                parent_job = ParentRunMetadata(
                    run_id=parent_run_id,
                    job_name=job_name if job_name is not None else f"{dag_id}.{task_id}",
                    job_namespace=_DAG_NAMESPACE,
                )
                processor.dbt_run_metadata = parent_job

            events = processor.parse().events()
            client = OpenLineageAdapter.get_or_create_openlineage_client()
            for event in events:
                client.emit(event)

    async def get_dbt_artifacts(self, hook, run_id, account_id, step, artifacts, headers, tenant):
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = {
                artifact: DbtCloudExtractor.get_job_run_artifact(
                    hook=hook,
                    session=session,
                    tenant=tenant,
                    run_id=run_id,
                    path=artifact,
                    account_id=account_id,
                    step=step,
                )
                for artifact in artifacts
            }
            results = await asyncio.gather(*tasks.values())
        return {filename: result for filename, result in zip(tasks.keys(), results)}

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        task_meta_data = self.get_task_metadata()
        operator = self.operator
        if (
            operator.task_type in ["DbtCloudRunJobOperator", "DbtCloudRunJobOperatorAsync"]
            and operator.wait_for_termination is True
        ):
            run_id = task_instance.xcom_pull(task_ids=task_instance.task_id, key="return_value")
            if not run_id:
                run_url = task_instance.xcom_pull(task_ids=task_instance.task_id, key="job_run_url")
                run_id = run_url.split("/")[-2]
        elif operator.task_type in [
            "DbtCloudJobRunSensor",
            "DbtCloudJobRunSensorAsync",
        ]:
            run_id = operator.run_id

        self.extract_dbt_lineage(
            operator=operator,
            run_id=run_id,
            job_name=f"{task_instance.dag_id}.{task_instance.task_id}",
        )

        return task_meta_data

    @staticmethod
    def get_headers_tenants_from_connection(hook):
        """Get Headers, tenants from the connection details"""
        headers = {}
        connection = hook.get_connection(hook.dbt_cloud_conn_id)
        tenant = connection.host or "cloud.getdbt.com"
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = f"Token {connection.password}"
        return headers, tenant

    @staticmethod
    @fallback_to_default_account
    async def get_job_run_artifact(hook, session, tenant, run_id: int, path: str, account_id=None, step=None):
        endpoint = f"{account_id}/runs/{run_id}/artifacts/{path}.json"
        url = f"https://{tenant}/api/v2/accounts/{endpoint or ''}"
        async with session.get(url, params={"step": step}) as response:
            try:
                response.raise_for_status()
                return await response.json(content_type=None)
            except aiohttp.ClientResponseError as e:
                raise Exception(str(e.status) + ":" + e.message)
