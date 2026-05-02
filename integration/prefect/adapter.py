# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import uuid
from uuid import uuid4
from datetime import datetime
from typing import List, Dict, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState

CLIENT: OpenLineageClient = OpenLineageClient.from_environment()
PRODUCER: str = 'https://github.com/OpenLineage/OpenLineage/tree/$VERSION/integration/prefect'
JOB_NAMESPACE: str = os.environ.get('OPENLINEAGE_NAMESPACE')

class PrefectOpenLineageAdapter:

    def generate_job_name(self, flow_name: str, task_name: str):
        return self.job_namespace + '.' + flow_name + '.' + task_name
    
    def create_and_emit_events(
        self, 
        eventType: str = None,
        eventTime: datetime = None,
        flowName: str = None,
        taskName: str = None,
    ) -> RunEvent:

        run_id: str = str(uuid.uuid4())

        if eventType == 'Running':
            eventType: RunState = RunState.START

        elif eventType == 'Complete':
            eventType: RunState = RunState.COMPLETE

        elif eventType == 'Failed':
            eventType: RunState = RunState.FAILED

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime.isoformat(),
            run=Run(run_id),
            job=Job(
                self.JOB_NAMESPACE, 
                self.generate_job_name(flowName, taskName),
            ),
            producer=PRODUCER
        )

        try:
            CLIENT.emit(run_event)
            print('OpenLineage event sent successfully')
        except:
            print('OpenLineage event not sent')
