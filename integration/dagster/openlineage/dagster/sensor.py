# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, Optional, Set, Union

from openlineage.dagster.adapter import OpenLineageAdapter
from openlineage.dagster.cursor import OpenLineageCursor, RunningPipeline, RunningStep
from openlineage.dagster.utils import (
    get_event_log_records,
    get_repository_name,
    make_step_run_id,
)

from dagster import (  # type: ignore
    DagsterEventType,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from dagster.core.definitions.sensor_definition import DEFAULT_SENSOR_DAEMON_INTERVAL
from dagster.core.events import PIPELINE_EVENTS, STEP_EVENTS

_ADAPTER = OpenLineageAdapter()

log = logging.getLogger(__name__)


def openlineage_sensor(
    name: Optional[str] = "openlineage_sensor",
    description: Optional[str] = "OpenLineage sensor tails Dagster event logs, "
    "converts Dagster events into OpenLineage events, "
    "and emits them to an OpenLineage backend.",
    concerned_event_types: Union[DagsterEventType, Set[DagsterEventType]] = PIPELINE_EVENTS | STEP_EVENTS,
    minimum_interval_seconds: Optional[int] = DEFAULT_SENSOR_DAEMON_INTERVAL,
    record_filter_limit: Optional[int] = 30,
    after_storage_id: Optional[int] = 0,
) -> SensorDefinition:
    """Wrapper to parameterize sensor configurations and return sensor definition.
    :param name: sensor name
    :param description: sensor description
    :param concerned_event_types: event types to filter out event records
    :param minimum_interval_seconds: minimum number of seconds that will elapse between evaluations
    :param record_filter_limit: maximum number of event logs to process on each evaluation
    :param after_storage_id: storage id to use as the initial after cursor when getting event logs
    :return: OpenLineage sensor definition
    """

    @sensor(  # type: ignore
        name=name,
        minimum_interval_seconds=minimum_interval_seconds,
        description=description,
    )
    def _openlineage_sensor(context: SensorEvaluationContext):
        # cursor keeps track of the last_storage_id checkpoint and running_pipelines, a map of
        # pipeline run ids to dynamically generated/extracted metadata for running pipelines
        ol_cursor = (
            OpenLineageCursor.from_json(context.cursor)
            if context.cursor
            else OpenLineageCursor(after_storage_id)
        )
        last_storage_id = ol_cursor.last_storage_id
        running_pipelines = ol_cursor.running_pipelines

        event_log_records = get_event_log_records(
            context.instance,
            concerned_event_types,
            last_storage_id,
            record_filter_limit,
        )

        raised_exception = None
        for record in event_log_records:
            entry = record.event_log_entry
            if entry.is_dagster_event:
                try:
                    pipeline_name = entry.job_name
                    pipeline_run_id = entry.run_id
                    timestamp = entry.timestamp
                    dagster_event = entry.get_dagster_event()
                    dagster_event_type = dagster_event.event_type
                    step_key = dagster_event.step_key

                    running_pipeline = running_pipelines.get(pipeline_run_id)
                    repository_name = (
                        running_pipeline.repository_name
                        if running_pipeline
                        else get_repository_name(context.instance, pipeline_run_id)
                    )

                    if dagster_event_type in PIPELINE_EVENTS:
                        _handle_pipeline_event(
                            running_pipelines,
                            dagster_event_type,
                            pipeline_name,
                            pipeline_run_id,
                            timestamp,
                            repository_name,
                        )
                    elif dagster_event_type in STEP_EVENTS:
                        _handle_step_event(
                            running_pipelines,
                            dagster_event_type,
                            pipeline_name,
                            pipeline_run_id,
                            timestamp,
                            step_key,
                            repository_name,
                        )
                except Exception as e:
                    # On failure, break and terminate evaluation
                    raised_exception = e
                    break
            last_storage_id = record.storage_id

        _update_cursor(context, last_storage_id, running_pipelines)

        if not raised_exception:
            msg = f"Last cursor: {context.cursor}"
        else:
            msg = f"Sensor run failed with error: {raised_exception}. " f"Last cursor: {context.cursor}"
        log.info(msg)
        yield SkipReason(msg)

    return _openlineage_sensor


def _handle_pipeline_event(
    running_pipelines: Dict[str, RunningPipeline],
    dagster_event_type: DagsterEventType,
    pipeline_name: str,
    pipeline_run_id: str,
    timestamp: float,
    repository_name: Optional[str],
):
    """Handles pipeline events that are of type RUN_START, RUN_SUCCESS, RUN_FAILURE,
    and RUN_CANCELED. Assumes event type is always in the order of RUN_START
    followed by RUN_SUCCESS, RUN_FAILURE, or RUN_CANCELED.

    :param running_pipelines: map of pipeline run ids to dynamically generated metadata
                              for pipelines that are in progress between sensor evaluations.
    :param dagster_event_type: Dagster pipeline event type
    :param pipeline_name: Dagster pipeline name
    :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
    :param timestamp: Unix timestamp of Dagster event
    :param repository_name: Dagster repository name
    :return:
    """
    if dagster_event_type == DagsterEventType.RUN_START:
        _ADAPTER.start_pipeline(pipeline_name, pipeline_run_id, timestamp, repository_name)
        running_pipelines[pipeline_run_id] = RunningPipeline(repository_name=repository_name)
    elif dagster_event_type == DagsterEventType.RUN_SUCCESS:
        _ADAPTER.complete_pipeline(pipeline_name, pipeline_run_id, timestamp, repository_name)
        running_pipelines.pop(pipeline_run_id, None)
    elif dagster_event_type == DagsterEventType.RUN_FAILURE:
        _ADAPTER.fail_pipeline(pipeline_name, pipeline_run_id, timestamp, repository_name)
        running_pipelines.pop(pipeline_run_id, None)
    elif dagster_event_type == DagsterEventType.RUN_CANCELED:
        _ADAPTER.cancel_pipeline(pipeline_name, pipeline_run_id, timestamp, repository_name)
        running_pipelines.pop(pipeline_run_id, None)


def _handle_step_event(
    running_pipelines: Dict[str, RunningPipeline],
    dagster_event_type: DagsterEventType,
    pipeline_name: str,
    pipeline_run_id: str,
    timestamp: float,
    step_key: str,
    repository_name: Optional[str],
):
    """Handles step events that are of type STEP_START, STEP_SUCCESS, and STEP_FAILURE.
    Assumes event type is always in the order of STEP_START
    followed by STEP_SUCCESS or STEP_FAILURE.

    :param running_pipelines: map of pipeline run ids to dynamically generated metadata
                              for running pipelines.
    :param dagster_event_type: Dagster pipeline event type
    :param pipeline_name: Dagster pipeline name
    :param pipeline_run_id: Dagster-generated unique identifier for a pipeline run
    :param timestamp: Unix timestamp of Dagster event
    :param step_key: Dagster step key
    :param repository_name: Dagster repository name
    :return:
    """
    running_pipeline = running_pipelines.get(
        pipeline_run_id, RunningPipeline(repository_name=repository_name)
    )
    running_steps = running_pipeline.running_steps
    running_step = running_steps.get(step_key, RunningStep(make_step_run_id()))
    step_run_id = running_step.step_run_id

    if dagster_event_type == DagsterEventType.STEP_START:
        _ADAPTER.start_step(
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )
        running_steps[step_key] = running_step
    elif dagster_event_type == DagsterEventType.STEP_SUCCESS:
        _ADAPTER.complete_step(
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )
        running_steps.pop(step_key, None)
    elif dagster_event_type == DagsterEventType.STEP_FAILURE:
        _ADAPTER.fail_step(
            pipeline_name,
            pipeline_run_id,
            timestamp,
            step_run_id,
            step_key,
            repository_name,
        )
        running_steps.pop(step_key, None)
    running_pipelines[pipeline_run_id] = running_pipeline


def _update_cursor(
    context: SensorEvaluationContext,
    last_storage_id: int,
    running_pipelines: Dict[str, RunningPipeline],
):
    """Updates cursor for a given sensor evaluation context.
    :param context: sensor evaluation context
    :param last_storage_id: last process storage id
    :param running_pipelines: pipeline runs that are in progress in between sensor runs
                              for their state to be shared
    :return:
    """
    context.update_cursor(
        OpenLineageCursor(last_storage_id=last_storage_id, running_pipelines=running_pipelines).to_json()
    )
