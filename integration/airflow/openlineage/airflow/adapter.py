# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import uuid
from typing import TYPE_CHECKING, Dict, List, Optional, Type

import requests.exceptions
from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.utils import DagUtils, redact_with_exclusions
from openlineage.airflow.version import __version__ as OPENLINEAGE_AIRFLOW_VERSION
from openlineage.client import OpenLineageClient, OpenLineageClientOptions, set_producer
from openlineage.client.facet import (
    BaseFacet,
    DocumentationJobFacet,
    ErrorMessageRunFacet,
    NominalTimeRunFacet,
    OwnershipJobFacet,
    OwnershipJobFacetOwners,
    ParentRunFacet,
    ProcessingEngineRunFacet,
    SourceCodeLocationJobFacet,
)
from openlineage.client.run import Job, Run, RunEvent, RunState

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun


_DAG_DEFAULT_OWNER = "anonymous"
_DAG_DEFAULT_NAMESPACE = "default"

_DAG_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", None)
if not _DAG_NAMESPACE:
    _DAG_NAMESPACE = os.getenv("MARQUEZ_NAMESPACE", _DAG_DEFAULT_NAMESPACE)

_PRODUCER = (
    f"https://github.com/OpenLineage/OpenLineage/tree/"
    f"{OPENLINEAGE_AIRFLOW_VERSION}/integration/airflow"
)

set_producer(_PRODUCER)


log = logging.getLogger(__name__)


class OpenLineageAdapter:
    """
    Adapter for translating Airflow metadata to OpenLineage events,
    instead of directly creating them from Airflow code.
    """

    _client = None

    def get_or_create_openlineage_client(self) -> OpenLineageClient:
        if not self._client:

            # Backcomp with Marquez integration
            marquez_url = os.getenv("MARQUEZ_URL")
            marquez_api_key = os.getenv("MARQUEZ_API_KEY")
            if marquez_url:
                log.info(f"Sending lineage events to {marquez_url}")
                self._client = OpenLineageClient(
                    marquez_url, OpenLineageClientOptions(api_key=marquez_api_key)
                )
            else:
                self._client = OpenLineageClient.from_environment()
        return self._client

    def build_dag_run_id(self, dag_id, dag_run_id):
        return str(
            uuid.uuid3(uuid.NAMESPACE_URL, f"{_DAG_NAMESPACE}.{dag_id}.{dag_run_id}")
        )

    @staticmethod
    def build_task_instance_run_id(task_id, execution_date, try_number):
        return str(
            uuid.uuid3(
                uuid.NAMESPACE_URL,
                f"{_DAG_NAMESPACE}.{task_id}.{execution_date}.{try_number}",
            )
        )

    def emit(self, event: RunEvent):
        event = redact_with_exclusions(event)
        try:
            return self.get_or_create_openlineage_client().emit(event)
        except requests.exceptions.RequestException:
            log.exception(f"Failed to emit OpenLineage event of id {event.run.runId}")

    def start_task(
        self,
        run_id: str,
        job_name: str,
        job_description: str,
        event_time: str,
        parent_job_name: Optional[str],
        parent_run_id: Optional[str],
        code_location: Optional[str],
        nominal_start_time: str,
        nominal_end_time: str,
        owners: List[str],
        task: Optional[TaskMetadata],
        run_facets: Optional[Dict[str, Type[BaseFacet]]] = None,  # Custom run facets
    ) -> str:
        """
        Emits openlineage event of type START
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task in dag
        :param job_description: user provided description of job
        :param event_time:
        :param parent_job_name: the name of the parent job (typically the DAG,
                but possibly a task group)
        :param parent_run_id: identifier of job spawning this task
        :param code_location: file path or URL of DAG file
        :param nominal_start_time: scheduled time of dag run
        :param nominal_end_time: following schedule of dag run
        :param owners: list of owners of DAG
        :param task: metadata container with information extracted from operator
        :param run_facets: custom run facets
        :return:
        """
        from airflow.version import version as AIRFLOW_VERSION

        processing_engine_version_facet = ProcessingEngineRunFacet(
            version=AIRFLOW_VERSION,
            name="Airflow",
            openlineageAdapterVersion=OPENLINEAGE_AIRFLOW_VERSION,
        )

        run_facets["processing_engine"] = processing_engine_version_facet  # type: ignore
        event = RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=self._build_run(
                run_id,
                parent_job_name,
                parent_run_id,
                job_name,
                nominal_start_time,
                nominal_end_time,
                run_facets=run_facets,
            ),
            job=self._build_job(
                job_name=job_name,
                job_description=job_description,
                code_location=code_location,
                owners=owners,
                job_facets=task.job_facets if task else None,
            ),
            inputs=task.inputs if task else None,
            outputs=task.outputs if task else None,
            producer=_PRODUCER,
        )
        self.emit(event)
        return event.run.runId

    def complete_task(
        self, run_id: str, job_name: str, end_time: str, task: TaskMetadata
    ):
        """
        Emits openlineage event of type COMPLETE
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        """

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=end_time,
            run=self._build_run(run_id, run_facets=task.run_facets),
            job=self._build_job(job_name, job_facets=task.job_facets),
            inputs=task.inputs,
            outputs=task.outputs,
            producer=_PRODUCER,
        )
        self.emit(event)

    def fail_task(self, run_id: str, job_name: str, end_time: str, task: TaskMetadata):
        """
        Emits openlineage event of type FAIL
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=end_time,
            run=self._build_run(run_id, run_facets=task.run_facets),
            job=self._build_job(job_name),
            inputs=task.inputs,
            outputs=task.outputs,
            producer=_PRODUCER,
        )
        self.emit(event)

    def dag_started(
        self,
        dag_run: "DagRun",
        msg: str,
        nominal_start_time: str,
        nominal_end_time: str,
    ):
        event = RunEvent(
            eventType=RunState.START,
            eventTime=DagUtils.to_iso_8601(dag_run.start_date),
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE),
            run=self._build_run(
                run_id=self.build_dag_run_id(dag_run.dag_id, dag_run.run_id),
                nominal_start_time=nominal_start_time,
                nominal_end_time=nominal_end_time,
            ),
            inputs=[],
            outputs=[],
            producer=_PRODUCER,
        )
        self.emit(event)

    def dag_success(self, dag_run: "DagRun", msg: str):
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=DagUtils.to_iso_8601(dag_run.end_date),
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE),
            run=Run(runId=self.build_dag_run_id(dag_run.dag_id, dag_run.run_id)),
            inputs=[],
            outputs=[],
            producer=_PRODUCER,
        )
        self.emit(event)

    def dag_failed(self, dag_run: "DagRun", msg: str):
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=DagUtils.to_iso_8601(dag_run.end_date),
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE),
            run=Run(
                runId=self.build_dag_run_id(dag_run.dag_id, dag_run.run_id),
                facets={
                    "errorMessage": ErrorMessageRunFacet(
                        message=msg, programmingLanguage="python"
                    )
                },
            ),
            inputs=[],
            outputs=[],
            producer=_PRODUCER,
        )
        self.emit(event)

    @staticmethod
    def _build_run(
        run_id: str,
        parent_job_name: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        job_name: Optional[str] = None,
        nominal_start_time: Optional[str] = None,
        nominal_end_time: Optional[str] = None,
        run_facets: Dict[str, BaseFacet] = None,
    ) -> Run:
        facets = {}
        if nominal_start_time:
            facets.update({
                "nominalTime": NominalTimeRunFacet(
                    nominal_start_time, nominal_end_time
                )
            })
        if parent_run_id:
            parent_run_facet = ParentRunFacet.create(
                runId=parent_run_id,
                namespace=_DAG_NAMESPACE,
                name=parent_job_name or job_name,
            )
            facets.update({
                "parent": parent_run_facet,
                "parentRun": parent_run_facet,  # Keep sending this for the backward compatibility
            })

        if run_facets:
            facets.update(run_facets)

        return Run(run_id, facets)

    @staticmethod
    def _build_job(
        job_name: str,
        job_description: Optional[str] = None,
        code_location: Optional[str] = None,
        owners: List[str] = None,
        job_facets: Dict[str, BaseFacet] = None,
    ):
        facets = {}

        if job_description:
            facets.update({"documentation": DocumentationJobFacet(job_description)})
        if code_location:
            facets.update(
                {"sourceCodeLocation": SourceCodeLocationJobFacet("", code_location)}
            )
        if owners:
            facets.update(
                {
                    "ownership": OwnershipJobFacet(
                        owners=[OwnershipJobFacetOwners(name=owner) for owner in owners]
                    )
                }
            )
        if job_facets:
            facets = {**facets, **job_facets}

        return Job(_DAG_NAMESPACE, job_name, facets)
