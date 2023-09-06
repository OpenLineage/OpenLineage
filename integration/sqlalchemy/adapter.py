# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import uuid
from uuid import uuid4
from datetime import datetime
from typing import List, Dict, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.facet import SqlJobFacet
from openlineage.common.dataset import Dataset

client: OpenLineageClient = OpenLineageClient.from_environment() # client: OpenLineageClient = OpenLineageClient(url='http://host.docker.internal:5000')
producer: str = 'https://github.com/OpenLineage/OpenLineage/tree/$VERSION/integration/common/openlineage/provider/sqlalchemy'
job_namespace: str = os.environ.get('OPENLINEAGE_NAMESPACE')
app_name: str = os.environ.get('SQLALCHEMY_APP_NAME')

class OpenLineageAdapter:
    job_namespace: str = job_namespace   

    def generate_job_name(self, query_string: str):
        return self.job_namespace + '.' + app_name + '.' + query_string # to do
    
    def create_events(
        self, 
        datasets: List[Dataset], 
        query_string: str,
        start_eventTime: Optional[datetime] = None,
        complete_eventTime: Optional[datetime] = None
        ) -> RunEvent:

        print('creating OpenLineage events...')

        run_id: str = str(uuid.uuid4())
        if query_string == None:
            query_string = ''
        
        job_facets: Dict[str: SqlJobFacet(str)] = {
            'sql': SqlJobFacet(query=query_string)
            }

        if start_eventTime:
            eventType: RunState = RunState.START
            eventTime: datetime = start_eventTime
        else:
            eventType: RunState = RunState.COMPLETE
            eventTime: datetime = complete_eventTime

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime,
            run=Run(runId=run_id),
            job=Job(
                self.job_namespace, 
                self.generate_job_name(query_string), 
                facets=job_facets
                ),
            inputs=datasets,
            outputs=[],
            producer=producer
            )

        try:
            client.emit(run_event)
            print('event emission succeeded')
        except:
            print('event emission failed')
        