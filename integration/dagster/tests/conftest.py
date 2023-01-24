# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
import uuid
from typing import Optional

from openlineage.dagster import __version__ as OPENLINEAGE_DAGSTER_VERSION
from pkg_resources import parse_version

from dagster import DagsterEvent, DagsterEventType, EventLogEntry, EventLogRecord, PipelineRun
from dagster.core.code_pointer import FileCodePointer
from dagster.core.definitions.reconstructable import ReconstructableRepository
from dagster.core.execution.plan.objects import StepFailureData, StepSuccessData
from dagster.core.host_representation import (
    ExternalPipelineOrigin,
    ExternalRepositoryOrigin,
    InProcessRepositoryLocationOrigin,
)
from dagster.version import __version__ as DAGSTER_VERSION

PRODUCER = f"https://github.com/OpenLineage/OpenLineage/tree/" \
           f"{OPENLINEAGE_DAGSTER_VERSION}/integration/dagster"


def make_test_event_log_record(
        event_type: Optional[DagsterEventType] = None,
        pipeline_name: str = "a_job",
        pipeline_run_id: str = str(uuid.uuid4()),
        timestamp: float = time.time(),
        step_key: Optional[str] = None,
        storage_id: int = 1,
):
    # handle removed message field from EventLogEntry since 0.14.3 by https://github.com/dagster-io/dagster/pull/6769 # noqa: E501
    if parse_version(DAGSTER_VERSION) >= parse_version("0.14.3"):
        event_log_entry = EventLogEntry(
            None, "debug", "user_msg", pipeline_run_id, timestamp, step_key, pipeline_name,
            _make_dagster_event(event_type, pipeline_name, step_key) if event_type else None)
    else:
        event_log_entry = EventLogEntry(
            None, "msg", "debug", "user_msg", pipeline_run_id, timestamp, step_key, pipeline_name,
            _make_dagster_event(event_type, pipeline_name, step_key) if event_type else None)

    return EventLogRecord(
        storage_id=storage_id,
        event_log_entry=event_log_entry
    )


def _make_dagster_event(
        event_type: DagsterEventType,
        pipeline_name: str,
        step_key: str
):
    event_specific_data = None
    if event_type == DagsterEventType.STEP_SUCCESS:
        event_specific_data = StepSuccessData(duration_ms=1.0)
    elif event_type == DagsterEventType.STEP_FAILURE:
        event_specific_data = StepFailureData(error=None, user_failure_data=None)
    return DagsterEvent(
        event_type.value,
        pipeline_name,
        step_key=step_key,
        event_specific_data=event_specific_data
    )


def make_pipeline_run_with_external_pipeline_origin(
        repository_name: str,
):
    return PipelineRun(
        pipeline_name="test",
        execution_plan_snapshot_id="123",
        external_pipeline_origin=ExternalPipelineOrigin(
            external_repository_origin=ExternalRepositoryOrigin(
                repository_location_origin=InProcessRepositoryLocationOrigin(
                    recon_repo=ReconstructableRepository(
                        pointer=FileCodePointer(
                            python_file="/openlineage/dagster/tests/test_pipelines/repo.py",
                            fn_name="define_demo_execution_repo",
                        ),
                    )
                ),
                repository_name=repository_name,
            ),
            pipeline_name="test"
        )
    )
