# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime

from facets.run_facets import PrefectDeploymentRunFacet
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet import (
    JobTypeJobFacet,
    NominalTimeRunFacet,
    ParentRunFacet,
)
from openlineage.client.facet_v2 import job_dependencies_run, processing_engine_run
from openlineage.client.run import Job, Run, RunEvent, RunState

PRODUCER: str = "https://github.com/OpenLineage/openlineage/integration/prefect"

logger: logging.Logger = logging.getLogger(__name__)


class PrefectOpenLineageAdapter:
    def __init__(self, client: OpenLineageClient | None = None):
        self.client = client or OpenLineageClient()

    def create_and_emit_flow_event(
        self,
        run_id: str,
        event_type: str,
        event_time: datetime,
        flow_name: str | None = None,
        flow_namespace: str | None = None,
        prefect_version: str | None = None,
        deployment_id: str | None = None,
        deployment_created: str | None = None,
        deployment_updated: str | None = None,
        deployment_name: str | None = None,
    ) -> RunEvent:
        """Create and emit a flow-level OpenLineage event."""

        match event_type:
            case "START":
                event_type = RunState.START
            case "COMPLETE":
                event_type = RunState.COMPLETE
            case "FAILED":
                event_type = RunState.FAIL

        if deployment_id:
            run_facets = {
                "prefectDeployment": PrefectDeploymentRunFacet(
                    deploymentId=deployment_id,
                    created=deployment_created,
                    updated=deployment_updated,
                    name=deployment_name,
                ),
                "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                    version=prefect_version, name="Prefect"
                ),
            }
        else:
            run_facets = {
                "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                    version=prefect_version, name="Prefect"
                )
            }

        job_facets = {
            "jobType": JobTypeJobFacet(
                processingType="BATCH", integration="Prefect", jobType="FLOW"
            )
        }

        run_event = RunEvent(
            eventType=event_type,
            eventTime=event_time.isoformat(),
            run=Run(run_id, run_facets),
            job=Job(flow_namespace, flow_name, job_facets),
            producer=PRODUCER,
        )

        try:
            self.client.emit(run_event)
            logger.info("Emitted OpenLineage event successfully.")
        except Exception:
            logger.exception("OpenLineage event not sent.")

    def create_and_emit_task_event(
        self,
        run_id: str,
        event_type: str,
        event_time: datetime,
        expectedevent_time: datetime | None = None,
        flow_run_id: str | None = None,
        flow_name: str | None = None,
        task_name: str | None  = None,
        namespace: str | None  = None,
        job_deps: list | None = None,
        prefect_version: str | None = None,
        deployment_id: str | None = None,
        deployment_created: str | None = None,
        deployment_updated: str | None = None,
        deployment_name: str | None = None,
        input_datasets: list | None = None,
        output_datasets: list | None = None,
    ) -> RunEvent:
        """Create and emit a task-level OpenLineage event."""

        match event_type:
            case "START":
                event_type = RunState.START
            case "COMPLETE":
                event_type = RunState.COMPLETE
            case "FAILED":
                event_type = RunState.FAIL

        if deployment_id:
            run_facets = {
                "nominalTime": NominalTimeRunFacet(nominalStartTime=expectedevent_time),
                "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                    version=prefect_version, name="Prefect"
                ),
                "parentRun": ParentRunFacet(
                    run={"run_id": flow_run_id},
                    job={"namespace": namespace, "name": flow_name},
                ),
                "prefectDeployment": PrefectDeploymentRunFacet(
                    deploymentId=deployment_id,
                    created=deployment_created,
                    updated=deployment_updated,
                    name=deployment_name,
                ),
            }
        else:
            run_facets = {
                "nominalTime": NominalTimeRunFacet(nominalStartTime=expectedevent_time),
                "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                    version=prefect_version, name="Prefect"
                ),
                "parentRun": ParentRunFacet(
                    run={"runId": flow_run_id},
                    job={"namespace": namespace, "name": flow_name},
                ),
            }

        if job_deps:
            upstream_jobs = [
                job_dependencies_run.JobDependency(
                    job=job_dependencies_run.JobIdentifier(
                        namespace=dep["namespace"], name=dep["name"]
                    )
                )
                for dep in job_deps
            ]
            run_facets["jobDependencies"] = (
                job_dependencies_run.JobDependenciesRunFacet(upstream=upstream_jobs)
            )

        job_facets = {
            "jobType": JobTypeJobFacet(
                processingType="BATCH", integration="Prefect", jobType="TASK"
            )
        }

        inputs = [
            Dataset(namespace=dataset["uri"], name=dataset["table"])
            for dataset in input_datasets
        ]
        outputs = [
            Dataset(namespace=dataset["uri"], name=dataset["table"])
            for dataset in output_datasets
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
