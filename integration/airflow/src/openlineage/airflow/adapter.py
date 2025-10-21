# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional

from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.utils import DagUtils, redact_with_exclusions
from openlineage.airflow.version import __version__ as OPENLINEAGE_AIRFLOW_VERSION
from openlineage.client import OpenLineageClient, OpenLineageClientOptions, set_producer
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    JobFacet,
    RunFacet,
    documentation_job,
    error_message_run,
    job_type_job,
    nominal_time_run,
    ownership_job,
    parent_run,
    processing_engine_run,
    source_code_location_job,
)
from openlineage.client.uuid import generate_static_uuid

from airflow.stats import Stats

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun


_DAG_DEFAULT_OWNER = "anonymous"
_DAG_DEFAULT_NAMESPACE = "default"

_DAG_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", os.getenv("MARQUEZ_NAMESPACE", _DAG_DEFAULT_NAMESPACE))

_PRODUCER = (
    f"https://github.com/OpenLineage/OpenLineage/tree/{OPENLINEAGE_AIRFLOW_VERSION}/integration/airflow"
)

set_producer(_PRODUCER)

# https://openlineage.io/docs/spec/facets/job-facets/job-type
# They must be set after the `set_producer(_PRODUCER)`
# otherwise the `JobTypeJobFacet._producer` will be set with the default value
_JOB_TYPE_DAG = job_type_job.JobTypeJobFacet(jobType="DAG", integration="AIRFLOW", processingType="BATCH")
_JOB_TYPE_TASK = job_type_job.JobTypeJobFacet(jobType="TASK", integration="AIRFLOW", processingType="BATCH")

log = logging.getLogger(__name__)


class OpenLineageAdapter:
    """
    Adapter for translating Airflow metadata to OpenLineage events,
    instead of directly creating them from Airflow code.
    """

    _client = None

    def __init__(self):
        if "OPENLINEAGE_AIRFLOW_LOGGING" in os.environ:
            logging.getLogger(__name__.rpartition(".")[0]).setLevel(os.getenv("OPENLINEAGE_AIRFLOW_LOGGING"))

    @staticmethod
    def get_or_create_openlineage_client() -> OpenLineageClient:
        # Backcomp with Marquez integration
        marquez_url = os.getenv("MARQUEZ_URL")
        if marquez_url:
            log.info("Sending lineage events to %s", marquez_url)
            client = OpenLineageClient(
                marquez_url,
                OpenLineageClientOptions(api_key=os.environ["MARQUEZ_API_KEY"]),
            )
        else:
            client = OpenLineageClient()
        return client

    @property
    def client(self) -> OpenLineageClient:
        if not self._client:
            self._client = self.get_or_create_openlineage_client()
        return self._client

    @staticmethod
    def build_dag_run_id(dag_id: str, execution_date: datetime) -> str:
        return str(
            generate_static_uuid(
                instant=execution_date,
                data=f"{_DAG_NAMESPACE}.{dag_id}".encode("utf-8"),
            )
        )

    @staticmethod
    def build_task_instance_run_id(
        dag_id: str,
        task_id: str,
        try_number: int,
        execution_date: datetime,
    ) -> str:
        return str(
            generate_static_uuid(
                instant=execution_date,
                data=f"{_DAG_NAMESPACE}.{dag_id}.{task_id}.{try_number}".encode("utf-8"),
            )
        )

    def emit(self, event: RunEvent):
        event = redact_with_exclusions(event)
        try:
            with Stats.timer("ol.emit.attempts"):
                return self.client.emit(event)
        except Exception as e:
            Stats.incr("ol.emit.failed")
            log.exception("Failed to emit OpenLineage event of id %s", event.run.runId)
            log.debug(e)

    def close(self, timeout: float = -1) -> bool:
        if self._client:
            return self._client.close(timeout)
        return True

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
        run_facets: Optional[Dict[str, RunFacet]] = None,  # Custom run facets
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

        processing_engine_version_facet = processing_engine_run.ProcessingEngineRunFacet(
            version=AIRFLOW_VERSION,
            name="Airflow",
            openlineageAdapterVersion=OPENLINEAGE_AIRFLOW_VERSION,
        )
        if run_facets is None:
            run_facets = {}
        if task:
            run_facets = {**task.run_facets, **run_facets}
        run_facets["processing_engine"] = processing_engine_version_facet  # type: ignore
        event = RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=self._build_run(
                run_id=run_id,
                parent_job_name=parent_job_name,
                parent_run_id=parent_run_id,
                job_name=job_name,
                nominal_start_time=nominal_start_time,
                nominal_end_time=nominal_end_time,
                run_facets=run_facets,
            ),
            job=self._build_job(
                job_name=job_name,
                job_description=job_description,
                code_location=code_location,
                owners=owners,
                job_facets=task.job_facets if task else None,
                job_type=_JOB_TYPE_TASK,
            ),
            inputs=task.inputs if task else [],
            outputs=task.outputs if task else [],
        )
        self.emit(event)
        return event.run.runId

    def complete_task(
        self,
        run_id: str,
        parent_job_name: Optional[str],
        parent_run_id: Optional[str],
        job_name: str,
        end_time: str,
        task: TaskMetadata,
    ):
        """
        Emits openlineage event of type COMPLETE
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param parent_job_name: the name of the parent job (typically the DAG,
                but possibly a task group)
        :param parent_run_id: identifier of job spawning this task
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        """

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=end_time,
            run=self._build_run(
                run_id=run_id,
                parent_job_name=parent_job_name,
                parent_run_id=parent_run_id,
                run_facets=task.run_facets,
            ),
            job=self._build_job(job_name, job_facets=task.job_facets, job_type=_JOB_TYPE_TASK),
            inputs=task.inputs,
            outputs=task.outputs,
        )
        self.emit(event)

    def fail_task(
        self,
        run_id: str,
        job_name: str,
        parent_job_name: Optional[str],
        parent_run_id: Optional[str],
        end_time: str,
        task: TaskMetadata,
    ):
        """
        Emits openlineage event of type FAIL
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param parent_job_name: the name of the parent job (typically the DAG,
                but possibly a task group)
        :param parent_run_id: identifier of job spawning this task
        :param end_time: time of task completion
        :param task: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=end_time,
            run=self._build_run(
                run_id=run_id,
                parent_job_name=parent_job_name,
                parent_run_id=parent_run_id,
                run_facets=task.run_facets,
            ),
            job=self._build_job(job_name, job_facets=task.job_facets, job_type=_JOB_TYPE_TASK),
            inputs=task.inputs,
            outputs=task.outputs,
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
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE, facets={"jobType": _JOB_TYPE_DAG}),
            run=self._build_run(
                run_id=self.build_dag_run_id(
                    dag_id=dag_run.dag_id,
                    execution_date=dag_run.execution_date,
                ),
                nominal_start_time=nominal_start_time,
                nominal_end_time=nominal_end_time,
            ),
            inputs=[],
            outputs=[],
        )
        self.emit(event)

    def dag_success(self, dag_run: "DagRun", msg: str):
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=DagUtils.to_iso_8601(dag_run.end_date),
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE, facets={"jobType": _JOB_TYPE_DAG}),
            run=Run(
                runId=self.build_dag_run_id(
                    dag_id=dag_run.dag_id,
                    execution_date=dag_run.execution_date,
                ),
            ),
            inputs=[],
            outputs=[],
        )
        self.emit(event)

    def dag_failed(self, dag_run: "DagRun", msg: str):
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=DagUtils.to_iso_8601(dag_run.end_date),
            job=Job(name=dag_run.dag_id, namespace=_DAG_NAMESPACE, facets={"jobType": _JOB_TYPE_DAG}),
            run=Run(
                runId=self.build_dag_run_id(dag_id=dag_run.dag_id, execution_date=dag_run.execution_date),
                facets={
                    "errorMessage": error_message_run.ErrorMessageRunFacet(
                        message=msg, programmingLanguage="python"
                    )
                },
            ),
            inputs=[],
            outputs=[],
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
        run_facets: Optional[Dict[str, RunFacet]] = None,
    ) -> Run:
        facets: Dict[str, RunFacet] = {}
        if nominal_start_time:
            facets.update(
                {"nominalTime": nominal_time_run.NominalTimeRunFacet(nominal_start_time, nominal_end_time)}
            )
        parent_name = parent_job_name or job_name
        if parent_run_id is not None and parent_name is not None:
            parent_run_facet = parent_run.ParentRunFacet(
                run=parent_run.Run(runId=parent_run_id),
                job=parent_run.Job(namespace=_DAG_NAMESPACE, name=parent_name),
            )
            facets.update(
                {
                    "parent": parent_run_facet,
                }
            )

        if run_facets:
            facets.update(run_facets)

        return Run(run_id, facets)

    @staticmethod
    def _build_job(
        job_name: str,
        job_type: job_type_job.JobTypeJobFacet,
        job_description: Optional[str] = None,
        code_location: Optional[str] = None,
        owners: Optional[List[str]] = None,
        job_facets: Optional[Dict[str, JobFacet]] = None,
    ):
        facets: Dict[str, JobFacet] = {}

        if job_description:
            facets.update({"documentation": documentation_job.DocumentationJobFacet(job_description)})
        if code_location:
            facets.update(
                {"sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet("", code_location)}
            )
        if owners:
            facets.update(
                {
                    "ownership": ownership_job.OwnershipJobFacet(
                        owners=[ownership_job.Owner(name=owner) for owner in owners]
                    )
                }
            )
        if job_facets:
            facets = {**facets, **job_facets}

        facets.update({"jobType": job_type})

        return Job(_DAG_NAMESPACE, job_name, facets)
