# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import datetime
import os

from openlineage.client import OpenLineageClient
from openlineage.client.facet import ( JobTypeJobFacet )
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_static_uuid

PRODUCER: str = (
    "https://github.com/OpenLineage/openlineage/integration/temporal"
)

NAMESPACE = os.environ.get("TEMPORAL_OPENLINEAGE_NAMESPACE", "default")

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
