# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
from abc import ABC, abstractmethod
from typing import Optional

from openlineage.client.uuid import generate_new_uuid
from openlineage.dagster import __version__ as OPENLINEAGE_DAGSTER_VERSION
from packaging.version import Version

from dagster import (
    DagsterEvent,
    DagsterEventType,
    DagsterRun,
    EventLogEntry,
    EventLogRecord,
)
from dagster.core.execution.plan.objects import StepFailureData, StepSuccessData
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
            ExternalRepositoryOrigin,
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
            ExternalRepositoryOrigin,
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


class DagsterRunLE1_6_9Provider(DagsterRunProvider):
    """Class for provisioning `dagster.DagsterRun` instance for Dagster
    versioin >= `1.3.3`.
    """

    def get_instance(self, repository_name: str) -> DagsterRun:
        from dagster.core.host_representation import (
            ExternalJobOrigin,
            ExternalRepositoryOrigin,
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


class DagsterRunLE1_7_xProvider(DagsterRunProvider):  
    """Class for provisioning `dagster.DagsterRun` instance for Dagster  
    version >= `1.7.0`, <= `1.7.x`.  
    """  
  
    def get_instance(self, repository_name: str) -> DagsterRun:  
        from dagster._core.remote_representation.origin import (  
            RemoteJobOrigin,  
            RemoteRepositoryOrigin,  
            InProcessCodeLocationOrigin,  
        )  
  
        dagster_run = DagsterRun(  
            job_name="test",  
            execution_plan_snapshot_id="123",  
            external_job_origin=RemoteJobOrigin(  # 1.7.x uses external_job_origin  
                repository_origin=RemoteRepositoryOrigin(  
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

class DagsterRunLE1_11_5Provider(DagsterRunProvider): 
    """Class for provisioning `dagster.DagsterRun` instance for Dagster      
    version >= `1.8.0`, <= `1.11.5`.  
    Uses dagster._core.remote_representation.origin  
    """
    
    
    def get_instance(self, repository_name: str) -> DagsterRun:    
        from dagster._core.remote_representation.origin import (    
            RemoteJobOrigin,    
            RemoteRepositoryOrigin,    
            InProcessCodeLocationOrigin,    
        )    
    
        dagster_run = DagsterRun(    
            job_name="test",    
            execution_plan_snapshot_id="123",    
            remote_job_origin=RemoteJobOrigin(    
                repository_origin=RemoteRepositoryOrigin(    
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

class DagsterRunLatestProvider(DagsterRunProvider):  
    """Class for provisioning `dagster.DagsterRun` instance for Dagster  
    version >= `1.11.6`.  
    Uses dagster._core.remote_origin (new path as of 1.11.6)  
    """  
      
    def get_instance(self, repository_name: str) -> DagsterRun:  
        from dagster._core.remote_origin import (  # NEW import path  
            RemoteJobOrigin,  
            RemoteRepositoryOrigin,  
            InProcessCodeLocationOrigin,  
        )  
          
        dagster_run = DagsterRun(  
            job_name="test",  
            execution_plan_snapshot_id="123",  
            remote_job_origin=RemoteJobOrigin(  # Same attribute name  
                repository_origin=RemoteRepositoryOrigin(  
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
    pipeline_run_id: Optional[str] = None,
    timestamp: Optional[float] = None,
    step_key: Optional[str] = None,
    storage_id: int = 1,
):
    event_log_entry = EventLogEntry(
        None,
        "debug",
        "user_msg",
        pipeline_run_id or str(generate_new_uuid()),
        timestamp or time.time(),
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
    parsed_dagster_version = Version(DAGSTER_VERSION)  
    dagster_run_provider: DagsterRunProvider = None  
    if not dagster_run_provider and parsed_dagster_version <= Version("1.2.2"):  
        dagster_run_provider = DagsterRunLE1_2_2Provider()  
    if not dagster_run_provider and parsed_dagster_version <= Version("1.3.2"):  
        dagster_run_provider = DagsterRunLE1_3_2Provider()  
    if not dagster_run_provider and parsed_dagster_version <= Version("1.6.9"):  
        dagster_run_provider = DagsterRunLE1_6_9Provider()  
    if not dagster_run_provider and parsed_dagster_version <= Version("1.8.0"):  
        dagster_run_provider = DagsterRunLE1_7_xProvider()  
    if not dagster_run_provider and parsed_dagster_version <= Version("1.11.5"):  
        dagster_run_provider = DagsterRunLE1_11_5Provider()  # Uses dagster._core.remote_representation.origin  
    if not dagster_run_provider:  
        dagster_run_provider = DagsterRunLatestProvider()  # Uses dagster._core.remote_origin for 1.11.6+  
    return dagster_run_provider.get_instance(repository_name)