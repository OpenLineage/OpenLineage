# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Advisory: This integration is experimental and in active development.

from datetime import datetime
import logging
import os
from typing import List

from openlineage.client import OpenLineageClient
from openlineage.client.facet import JobTypeJobFacet, ParentRunFacet
from openlineage.client.facet_v2 import ( job_dependencies_run )
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
      self.client = client or OpenLineageClient('http://localhost:5000')
      self.job_namespace = job_namespace or os.getenv("JOB_NAMESPACE", "default")

    def generate_job_name(self, flow_name: str, task_name: str):
        return flow_name + '.' + task_name
    
    def create_and_emit_event(
        self,
        runId: str = None,
        eventType: str = None,
        eventTime: datetime = None,
        flowRunId: str = None,
        flowName: str = None,
        flowNamespace: str = None,
        taskName: str = None,
        jobDeps: List = None
    ) -> RunEvent:

        match eventType:
            case 'RUNNING':
                eventType: RunState = RunState.START
            case 'COMPLETE':
                eventType: RunState = RunState.COMPLETE
            case 'FAILED':
                eventType: RunState = RunState.FAIL

        job_facets = {"jobType": JobTypeJobFacet(
            processingType="BATCH", 
            integration="Prefect", 
            jobType="TASK"
        )}
        
        def build_run_facets():
            if jobDeps:
                dep = jobDeps[0] # TODO: support multiple dependencies
                upstream_job = [job_dependencies_run.JobDependency(
                    job=job_dependencies_run.JobIdentifier(
                        namespace=dep["namespace"], 
                        name=dep["name"]
                    )
                )]
                return {"parentRun": ParentRunFacet.create(
                                flowRunId, flowNamespace, flowName
                            ),
                            "jobDependencies": job_dependencies_run.JobDependenciesRunFacet(
                                upstream = upstream_job
                            )
                        }
            else:
                return {"parentRun": ParentRunFacet.create(
                            flowRunId, flowNamespace, flowName
                        )}

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime.isoformat(),
            run=Run(runId, build_run_facets()),
            job=Job(
                self.job_namespace,
                self.generate_job_name(flowName, taskName),
                job_facets
            ),
            producer=PRODUCER
        )

        try:
            self.client.emit(run_event)
            logger.info('Emitted OpenLineage event successfully.')
        except Exception as e:
            logger.exception('Could not emit OpenLineage event.')
