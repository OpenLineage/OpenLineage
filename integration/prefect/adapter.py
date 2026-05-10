# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Advisory: This integration is experimental and in active development.

from datetime import datetime
import logging
import os
from typing import List, Dict, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.facet import JobTypeJobFacet, ParentRunFacet
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
      self.client = client or OpenLineageClient()
      self.job_namespace = job_namespace or os.getenv("JOB_NAMESPACE", "default")

    def generate_job_name(self, flow_name: str, task_name: str):
        return flow_name + '.' + task_name
    
    def create_and_emit_event(
        self,
        runId: str = None,
        eventType: str = None,
        eventTime: datetime = None,
        flowName: str = None,
        taskName: str = None,
        parentRuns: List = None
    ) -> RunEvent:

        match eventType:
            case 'RUNNING':
                eventType: RunState = RunState.START
            case 'COMPLETE':
                eventType: RunState = RunState.COMPLETE
            case 'FAILED':
                eventType: RunState = RunState.FAILED

        job_facets = {"jobType": JobTypeJobFacet(
            processingType="BATCH", 
            integration="Prefect", 
            jobType="TASK"
        )}

        def build_run_facets():
            if parentRuns:
                parent = parentRuns[0] # to do: support multiple parents
                run_id = parent["id"]
                run_namespace = parent["namespace"]
                run_name = parent["name"]
                return {"parentRun": ParentRunFacet.create(
                            run_id, run_namespace, run_name
                        )}
            else:
                return None

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime.isoformat(),
            run=Run(runId, build_run_facets()),
            job=Job(
                'prefect_test', # to do: replace with constant
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
