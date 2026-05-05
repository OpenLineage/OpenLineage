# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Advisory: This integration is experimental and in active development.

from datetime import datetime
import logging
import os
from typing import List, Dict, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.facet import JobTypeJobFacet
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid

CLIENT: OpenLineageClient = OpenLineageClient.from_environment()
PRODUCER: str = 'https://github.com/OpenLineage/OpenLineage/tree/$VERSION/integration/prefect'
JOB_NAMESPACE: str = os.environ.get('OPENLINEAGE_NAMESPACE')

logger: logging.Logger = logging.getLogger(__name__)

class PrefectOpenLineageAdapter:

    def generate_job_name(self, flow_name: str, task_name: str):
        return flow_name + '.' + task_name
    
    def create_and_emit_events(
        self, 
        eventType: str = None,
        eventTime: datetime = None,
        flowName: str = None,
        taskName: str = None,
    ) -> RunEvent:

        run_id: str = str(generate_new_uuid())

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

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime.isoformat(),
            run=Run(run_id),
            job=Job(
                'prefect_test', # to do: replace with constant
                self.generate_job_name(flowName, taskName),
                job_facets
            ),
            producer=PRODUCER
        )

        try:
            CLIENT.emit(run_event)
            logger.info('Emitted OpenLineage event successfully.')
        except Exception as e:
            logger.warning(f'Could not emit OpenLineage event. {e}')
