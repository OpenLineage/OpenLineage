# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
import uuid
from abc import ABC, abstractmethod
from typing import Optional

from openlineage.dagster import __version__ as OPENLINEAGE_DAGSTER_VERSION
from pkg_resources import parse_version

from dagster import (
    DagsterEvent,
    DagsterEventType,
    DagsterRun,
    EventLogEntry,
    EventLogRecord,
)
from dagster.core.execution.plan.objects import StepFailureData, StepSuccessData
from dagster.core.host_representation import ExternalRepositoryOrigin
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin
from dagster.version import __version__ as DAGSTER_VERSION

PRODUCER = (
    f"https://github.com/OpenLineage/OpenLineage/tree/" f"{OPENLINEAGE_DAGSTER_VERSION}/integration/dagster"
)


class DagsterRunProvider(ABC):
    """Abstract class for provisioning `dagster.DagsterRun` instance over various
    Dagster versions.
    """

    @abstractmethod
    def get_instance(self, repository_name: str) -> DagsterRun:
        raise NotImplementedError


class DagsterRunLE1_2_2Provider(DagsterRunProvider):
    """Class for provisioning `dagster.DagsterRun` instance for Dagster
    versioin >= `1.0.0`, <= `1.2.2`.
    """

    def get_instance(self, repository_name: str) -> DagsterRun:
        from dagster.core.host_representation import (  # type: ignore
            ExternalPipelineOrigin,
            InProcessRepositoryLocationOrigin,
        )

        dagster_run = DagsterRun(
            pipeline_name="test",
            execution_plan_snapshot_id="123",
            external_pipeline_origin=ExternalPipelineOrigin(
                external_repository_origin=ExternalRepositoryOrigin(
                    repository_location_origin=InProcessRepositoryLocationOrigin(
                        loadable_target_origin=LoadableTargetOrigin(
                            python_file="/openlineage/dagster/tests/test_pipelines/repo.py",
                        )
                    ),
                    repository_name=repository_name,
                ),
                pipeline_name="test",
            ),
        )

        return dagster_run


class DagsterRunLE1_3_2Provider(DagsterRunProvider):
    """Class for provisioning `dagster.DagsterRun` instance for Dagster
    versioin >= `1.2.3`,<= `1.3.2`.
    """

    def get_instance(self, repository_name: str) -> DagsterRun:
        from dagster.core.host_representation import (
            ExternalPipelineOrigin,
            InProcessCodeLocationOrigin,
        )

        dagster_run = DagsterRun(
            pipeline_name="test",
            execution_plan_snapshot_id="123",
            external_pipeline_origin=ExternalPipelineOrigin(
                external_repository_origin=ExternalRepositoryOrigin(
                    code_location_origin=InProcessCodeLocationOrigin(
                        loadable_target_origin=LoadableTargetOrigin(
                            python_file="/openlineage/dagster/tests/test_pipelines/repo.py",
                        )
                    ),
                    repository_name=repository_name,
                ),
                pipeline_name="test",
            ),
        )
        return dagster_run


class DagsterRunLatestProvider(DagsterRunProvider):
    """Class for provisioning `dagster.DagsterRun` instance for Dagster
    versioin >= `1.3.3`.

    If the tests were broken again due to API changes to `dagster.DagsterRun` in a future version,
    say `x.y.z`. One can identify the previous version of `x.y.z`, say `x.y.y`, and rename this
    class to `DagsterRunLEx_y_yProvider`. And then implement a brand new `DagsterRunLatestProvider`
    according to the new API spec.
    """

    def get_instance(self, repository_name: str) -> DagsterRun:
        from dagster.core.host_representation import (
            ExternalJobOrigin,
            InProcessCodeLocationOrigin,
        )

        dagster_run = DagsterRun(
            job_name="test",
            execution_plan_snapshot_id="123",
            external_job_origin=ExternalJobOrigin(
                external_repository_origin=ExternalRepositoryOrigin(
                    code_location_origin=InProcessCodeLocationOrigin(
                        loadable_target_origin=LoadableTargetOrigin(
                            python_file="/openlineage/dagster/tests/test_pipelines/repo.py",
                        )
                    ),
                    repository_name=repository_name,
                ),
                job_name="test",
            ),
        )
        return dagster_run


def make_test_event_log_record(
    event_type: Optional[DagsterEventType] = None,
    pipeline_name: str = "a_job",
    pipeline_run_id: str = str(uuid.uuid4()),
    timestamp: float = time.time(),
    step_key: Optional[str] = None,
    storage_id: int = 1,
):
    event_log_entry = EventLogEntry(
        None,
        "debug",
        "user_msg",
        pipeline_run_id,
        timestamp,
        step_key,
        pipeline_name,
        _make_dagster_event(event_type, pipeline_name, step_key) if event_type else None,
    )

    return EventLogRecord(storage_id=storage_id, event_log_entry=event_log_entry)


def _make_dagster_event(event_type: DagsterEventType, pipeline_name: str, step_key: str):
    event_specific_data = None
    if event_type == DagsterEventType.STEP_SUCCESS:
        event_specific_data = StepSuccessData(duration_ms=1.0)
    elif event_type == DagsterEventType.STEP_FAILURE:
        event_specific_data = StepFailureData(error=None, user_failure_data=None)
    return DagsterEvent(
        event_type.value,
        pipeline_name,
        step_key=step_key,
        event_specific_data=event_specific_data,
    )


def make_pipeline_run_with_external_pipeline_origin(
    repository_name: str,
):
    parsed_dagster_version = parse_version(DAGSTER_VERSION)
    dagster_run_provider: DagsterRunProvider = None
    if not dagster_run_provider and parsed_dagster_version <= parse_version("1.2.2"):
        dagster_run_provider = DagsterRunLE1_2_2Provider()
    if not dagster_run_provider and parsed_dagster_version <= parse_version("1.3.2"):
        dagster_run_provider = DagsterRunLE1_3_2Provider()
    if not dagster_run_provider:
        dagster_run_provider = DagsterRunLatestProvider()
    return dagster_run_provider.get_instance(repository_name)
