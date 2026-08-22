# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import datetime
from datetime import timezone
import logging

from openlineage.client import OpenLineageClient
from openlineage.client.facet import (
    JobTypeJobFacet
)
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_static_uuid
from temporalio.client import Client

PRODUCER: str = (
    "https://github.com/OpenLineage/openlineage/integration/temporal"
)

NAMESPACE = os.environ.get("TEMPORAL_OPENLINEAGE_NAMESPACE", "default")

logger: logging.Logger = logging.getLogger(__name__)

class TemporalOpenLineageAdapter:
    def __init__(self, client: OpenLineageClient | None = None):
        self.client = client or OpenLineageClient()

    def build_run_id(
        self, execution_time: datetime, run_name: str
    ) -> str:
        """Build a deterministic UUID for the OpenLineage run based on the execution time, run name, and namespace."""

        return str(
            generate_static_uuid(
                instant=execution_time,
                data=f"{NAMESPACE}.{run_name}".encode("utf-8"),
            )
        )

    def create_and_emit_task_event(
        self,
        runId: str,
        eventType: str,
        eventTime: datetime,
        taskName: str
    ) -> RunEvent:
        """Create and emit an OpenLineage task event."""

        job_facets = {
            "jobType": JobTypeJobFacet(
                processingType="BATCH", integration="Temporal", jobType="TASK"
            )
        }

        run_event = RunEvent(
            eventType=eventType,
            eventTime=eventTime.isoformat(),
            run=Run(runId),
            job=Job(NAMESPACE, taskName, job_facets),
            producer=PRODUCER,
        )
        self.client.emit(run_event)


async def get_temporal_events(workflow_ids: list, t_client: Client) -> None:
    adapter = TemporalOpenLineageAdapter()

    temporal_events = []
    for workflow_id in workflow_ids:
        description = await t_client.get_workflow_handle(workflow_id).describe()
        
        start_event_name = description.raw_info.type.name
        start_event_time = description.raw_info.start_time
        base_dt = datetime.datetime.fromtimestamp(start_event_time.seconds, tz=timezone.utc)
        start_final_dt = base_dt + datetime.timedelta(microseconds=start_event_time.nanos // 1000)
        start_event_run_id = adapter.build_run_id(start_final_dt, start_event_name)

        adapter.create_and_emit_task_event(start_event_run_id, RunState.START, start_final_dt, start_event_name)

        complete_event_name = description.raw_info.type.name
        complete_event_time = description.raw_info.close_time
        base_dt = datetime.datetime.fromtimestamp(complete_event_time.seconds, tz=timezone.utc)
        complete_final_dt = base_dt + datetime.timedelta(microseconds=complete_event_time.nanos // 1000)

        adapter.create_and_emit_task_event(start_event_run_id, RunState.COMPLETE, complete_final_dt, complete_event_name)
