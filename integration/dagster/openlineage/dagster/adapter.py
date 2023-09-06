# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from typing import Dict, Optional

from openlineage.client import OpenLineageClient, set_producer
from openlineage.client.constants import DEFAULT_NAMESPACE_NAME
from openlineage.client.facet import ParentRunFacet
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.dagster import __version__ as OPENLINEAGE_DAGSTER_VERSION
from openlineage.dagster.utils import make_step_job_name, to_utc_iso_8601

_DEFAULT_NAMESPACE_NAME = os.getenv("OPENLINEAGE_NAMESPACE", DEFAULT_NAMESPACE_NAME)
_PRODUCER = f"https://github.com/OpenLineage/OpenLineage/tree/" \
            f"{OPENLINEAGE_DAGSTER_VERSION}/integration/dagster"

set_producer(_PRODUCER)

log = logging.getLogger(__name__)


class OpenLineageAdapter:
    """Adapter to translate Dagster metadata to OpenLineage events
    and emit them to OpenLineage backend
    """

    def __init__(self):
        self._client = OpenLineageClient()

    def start_pipeline(
            self,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            repository_name: Optional[str] = None
    ):
        """Emits OpenLineage event to indicate start of Dagster pipeline run.
        :param pipeline_name: Dagster pipeline name
        :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
        :param timestamp: Unix timestamp of Dagster event
        :param repository_name: Dagster repository name
        :return:
        """
        self._emit_pipeline_event(
            RunState.START, pipeline_name, pipeline_run_id, timestamp, repository_name
        )

    def complete_pipeline(
            self,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            repository_name: Optional[str] = None
    ):
        """Emits OpenLineage event to indicate completion of Dagster pipeline run.
        :param pipeline_name: Dagster pipeline name
        :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
        :param timestamp: Unix timestamp of Dagster event
        :param repository_name: Dagster repository name
        :return:
        """
        self._emit_pipeline_event(
            RunState.COMPLETE, pipeline_name, pipeline_run_id, timestamp, repository_name
        )

    def fail_pipeline(
            self,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            repository_name: Optional[str] = None
    ):
        """Emits OpenLineage event to indicate failure of Dagster pipeline run.
        :param pipeline_name: Dagster pipeline name
        :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
        :param timestamp: Unix timestamp of Dagster event
        :param repository_name: Dagster repository name
        :return:
        """
        self._emit_pipeline_event(
            RunState.FAIL, pipeline_name, pipeline_run_id, timestamp, repository_name
        )

    def cancel_pipeline(
            self,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            repository_name: Optional[str] = None
    ):
        """Emits OpenLineage event to indicate cancellation of Dagster pipeline run.
        :param pipeline_name: Dagster pipeline name
        :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
        :param timestamp: Unix timestamp of Dagster event
        :param repository_name: Dagster repository name
        :return:
        """
        self._emit_pipeline_event(
            RunState.ABORT, pipeline_name, pipeline_run_id, timestamp, repository_name
        )

    def _emit_pipeline_event(
            self,
            event_type: RunState,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            repository_name: Optional[str],
    ):
        self._emit(
            RunEvent(
                eventType=event_type,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(
                    namespace=repository_name if repository_name else _DEFAULT_NAMESPACE_NAME,
                    run_id=pipeline_run_id
                ),
                job=self._build_job(
                    namespace=repository_name if repository_name else _DEFAULT_NAMESPACE_NAME,
                    job_name=pipeline_name
                ),
                producer=_PRODUCER
            ))

    def start_step(
            self,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            step_run_id: str,
            step_key: str,
            repository_name: Optional[str] = None
    ):
        """Emits OpenLineage event to indicate start of Dagster step (op) execution.
        :param pipeline_name: Dagster pipeline name
        :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
        :param timestamp: Unix timestamp of Dagster event
        :param step_run_id: Unique identifier for a step run (not associated with Dagster)
        :param step_key: Dagster step key
        :param repository_name: Dagster repository name
        :return:
        """
        self._emit_step_event(RunState.START, pipeline_name, pipeline_run_id, timestamp,
                              step_run_id, step_key, repository_name)

    def complete_step(
            self,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            step_run_id: str,
            step_key: str,
            repository_name: Optional[str] = None
    ):
        """Emits OpenLineage event to indicate completion of Dagster step (op) execution.
        :param pipeline_name: Dagster pipeline name
        :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
        :param timestamp: Unix timestamp of Dagster event
        :param step_run_id: Unique identifier for a step run (not associated with Dagster)
        :param step_key: Dagster step key
        :param repository_name: Dagster repository name
        :return:
        """
        self._emit_step_event(RunState.COMPLETE, pipeline_name, pipeline_run_id, timestamp,
                              step_run_id, step_key, repository_name)

    def fail_step(
            self,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            step_run_id: str,
            step_key: str,
            repository_name: Optional[str] = None
    ):
        """Emits OpenLineage event to indicate failure of Dagster step (op) execution.
        :param pipeline_name: Dagster pipeline name
        :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
        :param timestamp: Unix timestamp of Dagster event
        :param step_run_id: Unique identifier for a step run (not associated with Dagster)
        :param step_key: Dagster step key
        :param repository_name: Dagster repository name
        :return:
        """
        self._emit_step_event(RunState.FAIL, pipeline_name, pipeline_run_id, timestamp,
                              step_run_id, step_key, repository_name)

    def _emit_step_event(
            self,
            event_type: RunState,
            pipeline_name: str,
            pipeline_run_id: str,
            timestamp: float,
            step_run_id: str,
            step_key: str,
            repository_name: Optional[str]
    ):
        self._emit(
            RunEvent(
                eventType=event_type,
                eventTime=to_utc_iso_8601(timestamp),
                run=self._build_run(
                    namespace=repository_name if repository_name else _DEFAULT_NAMESPACE_NAME,
                    run_id=step_run_id,
                    parent_run_id=pipeline_run_id,
                    parent_job_name=pipeline_name
                ),
                job=self._build_job(
                    namespace=repository_name if repository_name else _DEFAULT_NAMESPACE_NAME,
                    job_name=make_step_job_name(pipeline_name, step_key)
                ),
                producer=_PRODUCER
            ))

    def _emit(self, event: RunEvent):
        self._client.emit(event)
        log.debug(f"Successfully emitted OpenLineage run event: {event}")

    @staticmethod
    def _build_run(
            namespace: str,
            run_id: str,
            parent_run_id: Optional[str] = None,
            parent_job_name: Optional[str] = None
    ) -> Run:
        facets: Dict = {}
        if parent_run_id is not None and parent_job_name is not None:
            facets.update({
                "parent": ParentRunFacet.create(parent_run_id, namespace, parent_job_name)
            })
        return Run(run_id, facets)

    @staticmethod
    def _build_job(
            namespace: str,
            job_name: str
    ) -> Job:
        facets: Dict = {}
        return Job(namespace, job_name, facets)
