# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import dataclass
from datetime import datetime

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import (
    error_message_run,
    job_dependencies_run,
    job_type_job,
    nominal_time_run,
    parent_run,
    processing_engine_run,
)
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage_prefect.prefect_adapter import facets

PRODUCER: str = "https://github.com/OpenLineage/openlineage/integration/prefect"

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class FlowInfo:
    start_time: datetime
    name: str
    namespace: str | None
    event_time: datetime | None


@dataclass
class DeploymentInfo:
    namespace: str
    flow_run: object
    id: str | None = None
    created: datetime | None = None
    updated: datetime | None = None
    name: str | None = None


@dataclass
class TaskInfo:
    run_id: str
    event_time: datetime
    name: str
    namespace: str
    event_type: RunState
    expected_start_time: datetime
    prefect_version: str | None = None
    job_deps: list | None = None
    state_msg: str | None = None


@dataclass
class DatasetInfo:
    input_datasets: list
    output_datasets: list


class PrefectOpenLineageAdapter:
    def __init__(self, client: OpenLineageClient | None = None):
        self.client = client or OpenLineageClient()

    def create_and_emit_flow_event(
        self,
        run_id: str,
        event_type: RunState,
        flow_info: FlowInfo,
        prefect_version: str | None = None,
        deployment: DeploymentInfo | None = None,
    ) -> None:
        """Create and emit a flow-level OpenLineage event."""

        if deployment:
            run_facets = {
                "prefectDeployment": facets.run_facets.PrefectDeploymentRunFacet(
                    deploymentId=deployment.id,
                    created=deployment.created.isoformat(),
                    updated=deployment.updated.isoformat(),
                    name=deployment.name,
                ),
                "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                    version=prefect_version if prefect_version else "unknown", name="Prefect"
                ),
            }
        else:
            run_facets = {
                "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                    version=prefect_version if prefect_version else "unknown", name="Prefect"
                ),
            }

        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                processingType="BATCH", integration="Prefect", jobType="FLOW"
            )
        }

        run_event = RunEvent(
            eventType=event_type,
            eventTime=flow_info.event_time.isoformat(),
            run=Run(run_id, run_facets),
            job=Job(flow_info.namespace, flow_info.name, job_facets),
            producer=PRODUCER,
        )

        try:
            self.client.emit(run_event)
            logger.info("Emitted OpenLineage event successfully.")
        except Exception:
            logger.exception("OpenLineage event not sent.")

    def create_and_emit_task_event(
        self,
        task_info: TaskInfo,
        flow_run_id: str | None = None,
        flow_name: str | None = None,
        deployment: DeploymentInfo | None = None,
        datasets: DatasetInfo | None = None,
    ) -> None:
        """Create and emit a task-level OpenLineage event."""

        run_id = task_info.run_id
        event_type = task_info.event_type
        event_time = task_info.event_time
        expected_start_time = task_info.expected_start_time
        task_name = task_info.name
        namespace = task_info.namespace
        prefect_version = task_info.prefect_version if task_info.prefect_version else "unknown"
        job_deps = task_info.job_deps if task_info.job_deps else []
        input_datasets = datasets.input_datasets if datasets else []
        output_datasets = datasets.output_datasets if datasets else []
        state_msg = task_info.state_msg if task_info.state_msg else ""

        if deployment:
            run_facets = {
                "nominalTime": nominal_time_run.NominalTimeRunFacet(nominalStartTime=expected_start_time),
                "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                    version=prefect_version, name="Prefect"
                ),
                "prefectDeployment": facets.run_facets.PrefectDeploymentRunFacet(
                    deploymentId=deployment.id,
                    created=deployment.created.isoformat(),
                    updated=deployment.updated.isoformat(),
                    name=deployment.name,
                ),
                "parentRun": parent_run.ParentRunFacet(
                    run={"runId": flow_run_id},
                    job={"namespace": namespace, "name": flow_name},
                )
                if flow_run_id
                else None,
                "errorMessageRun": error_message_run.ErrorMessageRunFacet(
                    message=state_msg,
                    programmingLanguage="Python",
                )
                if state_msg
                else None,
            }
        else:
            run_facets = {
                "nominalTime": nominal_time_run.NominalTimeRunFacet(nominalStartTime=expected_start_time),
                "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                    version=prefect_version, name="Prefect"
                ),
                "parentRun": parent_run.ParentRunFacet(
                    run={"runId": flow_run_id},
                    job={"namespace": namespace, "name": flow_name},
                ),
                "errorMessageRun": error_message_run.ErrorMessageRunFacet(
                    message=state_msg,
                    programmingLanguage="Python",
                )
                if state_msg
                else None,
            }

        if job_deps:
            upstream_jobs = [
                job_dependencies_run.JobDependency(
                    job=job_dependencies_run.JobIdentifier(namespace=dep["namespace"], name=dep["name"])
                )
                for dep in job_deps
            ]
            run_facets["jobDependencies"] = job_dependencies_run.JobDependenciesRunFacet(
                upstream=upstream_jobs
            )

        job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                processingType="BATCH", integration="Prefect", jobType="TASK"
            )
        }

        inputs = [
            Dataset(namespace=dataset["uri"], name=dataset["table"]) for dataset in input_datasets or []
        ]
        outputs = [
            Dataset(namespace=dataset["uri"], name=dataset["table"]) for dataset in output_datasets or []
        ]

        run_event = RunEvent(
            eventType=event_type,
            eventTime=event_time.isoformat(),
            run=Run(run_id, run_facets),
            job=Job(namespace, task_name, job_facets),
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        )

        try:
            self.client.emit(run_event)
            logger.info("Emitted OpenLineage event successfully.")
        except Exception:
            logger.exception("OpenLineage event not sent.")
