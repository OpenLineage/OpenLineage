# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Warning: this integration is experimental and in active development.

import datetime
import logging
import os

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet import JobTypeJobFacet
from openlineage.client.run import Job, Run, RunEvent
from openlineage.client.uuid import generate_static_uuid

PRODUCER: str = "https://github.com/OpenLineage/openlineage/integration/temporal"

NAMESPACE = os.environ.get("TEMPORAL_OPENLINEAGE_NAMESPACE", "default")

logger: logging.Logger = logging.getLogger(__name__)


class TemporalOpenLineageAdapter:
    def __init__(self, client: OpenLineageClient | None = None):
        self.client = client or OpenLineageClient()
        self.namespace = NAMESPACE
        self.producer = PRODUCER

    def build_run_id(self, execution_time: datetime, run_name: str) -> str:
        """
        Build a deterministic UUID for the OpenLineage run based on the
        execution time, run name, and namespace.
        """

        return str(
            generate_static_uuid(
                instant=execution_time,
                data=f"{self.namespace}.{run_name}".encode(),
            )
        )

    def create_and_emit_task_event(
        self,
        run_id: str,
        event_type: str,
        event_time: datetime,
        task_name: str,
        input_datasets: list | None = None,
        output_datasets: list | None = None,
    ) -> RunEvent:
        """Create and emit an OpenLineage task event."""

        job_facets = {
            "jobType": JobTypeJobFacet(
                processingType="BATCH", integration="Temporal", jobType="TASK"
            )
        }

        kwargs = {
            "event_type": event_type,
            "event_time": event_time.isoformat(),
            "run": Run(run_id),
            "job": Job(self.namespace, task_name, job_facets),
            "producer": self.producer,
        }

        inputs = [
            Dataset(namespace=dataset["uri"], name=dataset["table"])
            for dataset in input_datasets
        ]
        kwargs["inputs"] = inputs

        outputs = [
            Dataset(namespace=dataset["uri"], name=dataset["table"])
            for dataset in output_datasets
        ]
        kwargs["outputs"] = outputs

        run_event = RunEvent(**kwargs)
        self.client.emit(run_event)
