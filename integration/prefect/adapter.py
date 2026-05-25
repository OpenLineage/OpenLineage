# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Advisory: This integration is experimental and in active development.

from datetime import datetime
import logging
import os
from typing import List

from openlineage.client import OpenLineageClient
from openlineage.client.facet import JobTypeJobFacet, ParentRunFacet, NominalTimeRunFacet
from openlineage.client.facet_v2 import ( job_dependencies_run, processing_engine_run )
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid

PRODUCER: str = 'https://github.com/OpenLineage/OpenLineage/tree/$VERSION/integration/prefect'

logger: logging.Logger = logging.getLogger(__name__)

class PrefectOpenLineageAdapter:
    def __init__(
        self,
        client: OpenLineageClient | None = None,
        job_namespace: str | None = None,
    ):
        self.client = client or OpenLineageClient('http://localhost:5000') # for testing
    
    def create_and_emit_flow_event(
        self,
        runId: str = None,
        eventType: str = None,
        eventTime: datetime = None,
        flowName: str = None,
        flowNamespace: str = None,
        prefectVersion: str = None
    ) -> RunEvent:

        match eventType:
            case 'START':
                eventType: RunState = RunState.START
            case 'COMPLETE':
                eventType: RunState = RunState.COMPLETE
            case 'FAILED':
                eventType: RunState = RunState.FAIL

        run_facets = {"processingEngine": processing_engine_run.ProcessingEngineRunFacet(
            version=prefectVersion,
            name="Prefect"
        )}

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
        flowNamespace: str = None,
        taskName: str = None,
        taskNamespace: str = None,
        jobDeps: List = None,
        prefectVersion: str = None
    ) -> RunEvent:

        match eventType:
            case 'START':
                eventType: RunState = RunState.START
            case 'COMPLETE':
                eventType: RunState = RunState.COMPLETE
            case 'FAILED':
                eventType: RunState = RunState.FAIL
        
        def build_run_facets():
            if jobDeps:
                upstream_jobs = [
                    job_dependencies_run.JobDependency(
                        job=job_dependencies_run.JobIdentifier(
                            namespace=dep["namespace"], 
                            name=dep["name"]
                        )
                    ) for dep in jobDeps
                ]
                return {
                            "jobDependencies": job_dependencies_run.JobDependenciesRunFacet(
                                upstream=upstream_jobs
                            ),
                            "nominalTime": NominalTimeRunFacet(
                                nominalStartTime=expectedEventTime
                            ),
                            "parentRun": ParentRunFacet(
                                run={"runId": flowRunId},
                                job={"namespace": flowNamespace, "name": flowName}
                            ),
                            "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                                version=prefectVersion,
                                name="Prefect"
                            )
                        }
            else:
                return {
                            "nominalTime": NominalTimeRunFacet(
                                nominalStartTime=expectedEventTime
                            ),
                            "parentRun": ParentRunFacet(
                                run={"runId": flowRunId},
                                job={"namespace": flowNamespace, "name": flowName}
                            ),
                            "processingEngine": processing_engine_run.ProcessingEngineRunFacet(
                                version=prefectVersion,
                                name="Prefect"
                            )
                        }

        job_facets = {"jobType": JobTypeJobFacet(
            processingType="BATCH", 
            integration="Prefect", 
            jobType="TASK"
        )}

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime.isoformat(),
            run=Run(runId, build_run_facets()),
            job=Job(
                taskNamespace,
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
