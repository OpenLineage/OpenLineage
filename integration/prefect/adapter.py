# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Advisory: This integration is experimental and in active development.

import attr
from datetime import datetime
import json
import logging
import os

from facets.deploymentFacet import PrefectDeploymentRunFacet
from openlineage.client import OpenLineageClient
from openlineage.client.facet import BaseFacet, JobTypeJobFacet, ParentRunFacet, NominalTimeRunFacet
from openlineage.client.facet_v2 import ( job_dependencies_run, processing_engine_run )
from openlineage.client.run import Job, Run, RunEvent, RunState

PRODUCER: str = 'https://github.com/OpenLineage/OpenLineage/tree/$VERSION/integration/prefect'

logger: logging.Logger = logging.getLogger(__name__)

class PrefectOpenLineageAdapter:
    def __init__(
        self,
        client: OpenLineageClient | None = None
    ):
        self.client = client or OpenLineageClient('http://localhost:5000') # for testing
    
    def create_and_emit_flow_event(
        self,
        runId: str = None,
        eventType: str = None,
        eventTime: datetime = None,
        flowName: str = None,
        flowNamespace: str = None,
        prefectVersion: str = None,
        deploymentId: str = None,
        deploymentCreated: str = None,
        deploymentUpdated: str = None,
        deploymentName: str = None
    ) -> RunEvent:

        match eventType:
            case 'START':
                eventType: RunState = RunState.START
            case 'COMPLETE':
                eventType: RunState = RunState.COMPLETE
            case 'FAILED':
                eventType: RunState = RunState.FAIL

        run_facets = {
                        "prefectDeployment": PrefectDeploymentRunFacet(
                            deployment_id=deploymentId,
                            created=deploymentCreated,
                            updated=deploymentUpdated,
                            name=deploymentName
                        ),
                        "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                            version=prefectVersion,
                            name="Prefect"
                        )
                    }

        job_facets = {"jobType": JobTypeJobFacet(
            processingType="BATCH", 
            integration="Prefect",
            jobType="FLOW"
        )}

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime.isoformat(),
            run=Run(runId, run_facets),
            job=Job(
                flowNamespace,
                flowName,
                job_facets
            ),
            producer=PRODUCER
        )

        try:
            self.client.emit(run_event)
            logger.info('Emitted OpenLineage event successfully.')
        except Exception as e:
            logger.exception('Could not emit OpenLineage event.')

    def create_and_emit_task_event(
        self,
        runId: str = None,
        eventType: str = None,
        eventTime: datetime = None,
        expectedEventTime: datetime = None,
        flowRunId: str = None,
        flowName: str = None,
        taskName: str = None,
        namespace: str = None,
        jobDeps: list = None,
        prefectVersion: str = None,
        deploymentId: str = None,
        deploymentCreated: str = None,
        deploymentUpdated: str = None,
        deploymentName: str = None
    ) -> RunEvent:

        match eventType:
            case 'START':
                eventType: RunState = RunState.START
            case 'COMPLETE':
                eventType: RunState = RunState.COMPLETE
            case 'FAILED':
                eventType: RunState = RunState.FAIL

        
        run_facets = {
            "nominalTime": NominalTimeRunFacet(
                nominalStartTime=expectedEventTime
            ),
            "parentRun": ParentRunFacet(
                run={"runId": flowRunId},
                job={"namespace": namespace, "name": flowName}
            ),
            "prefectDeployment": PrefectDeploymentRunFacet(
                deployment_id=deploymentId,
                created=deploymentCreated,
                updated=deploymentUpdated,
                name=deploymentName
            ),
            "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                version=prefectVersion,
                name="Prefect"
            )
        }
        if jobDeps:
            upstream_jobs = [
                job_dependencies_run.JobDependency(
                    job=job_dependencies_run.JobIdentifier(
                        namespace=dep["namespace"],
                        name=dep["name"]
                    )
                ) for dep in jobDeps
            ]
            run_facets["jobDependencies"] = (
                job_dependencies_run.JobDependenciesRunFacet(upstream=upstream_jobs)
            )

        job_facets = {"jobType": JobTypeJobFacet(
            processingType="BATCH", 
            integration="Prefect", 
            jobType="TASK"
        )}

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime.isoformat(),
            run=Run(runId, run_facets),
            job=Job(
                namespace,
                taskName,
                job_facets
            ),
            producer=PRODUCER
        )

        try:
            self.client.emit(run_event)
            logger.info('Emitted OpenLineage event successfully.')
        except Exception as e:
            logger.exception('Could not emit OpenLineage event.')
