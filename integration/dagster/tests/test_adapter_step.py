# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
import uuid
from unittest.mock import patch

from openlineage.client.constants import DEFAULT_NAMESPACE_NAME
from openlineage.client.facet import ParentRunFacet
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.dagster.adapter import OpenLineageAdapter

from .conftest import PRODUCER


@patch("openlineage.dagster.adapter.to_utc_iso_8601")
@patch("openlineage.dagster.adapter.OpenLineageClient")
def test_start_pipeline(mock_client, mock_to_utc_iso_8601):
    event_time = "2022-01-01T00:00:00.000000Z"
    mock_to_utc_iso_8601.return_value = event_time

    pipeline_name = "a_pipeline"
    pipeline_run_id = str(uuid.uuid4())
    step_run_id = str(uuid.uuid4())
    step_key = "an_op"
    timestamp = time.time()

    adapter = OpenLineageAdapter()
    adapter.start_step(pipeline_name, pipeline_run_id, timestamp, step_run_id, step_key)

    mock_to_utc_iso_8601.assert_called_once_with(timestamp)
    mock_client.return_value.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=Run(
                runId=step_run_id,
                facets={
                    "parent": ParentRunFacet(
                        run={
                            "runId": pipeline_run_id
                        },
                        job={
                            "namespace": DEFAULT_NAMESPACE_NAME,
                            "name": pipeline_name
                        }
                    )
                }
            ),
            job=Job(
                namespace=DEFAULT_NAMESPACE_NAME,
                name=f"{pipeline_name}.{step_key}",
                facets={}
            ),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )
    )


@patch("openlineage.dagster.adapter.to_utc_iso_8601")
@patch("openlineage.dagster.adapter.OpenLineageClient")
def test_complete_step(mock_client, mock_to_utc_iso_8601):
    event_time = "2022-01-01T00:00:00.000000Z"
    mock_to_utc_iso_8601.return_value = event_time

    pipeline_name = "a_pipeline"
    pipeline_run_id = str(uuid.uuid4())
    step_run_id = str(uuid.uuid4())
    step_key = "an_op"
    timestamp = time.time()

    adapter = OpenLineageAdapter()
    adapter.complete_step(pipeline_name, pipeline_run_id, timestamp, step_run_id, step_key)

    mock_to_utc_iso_8601.assert_called_once_with(timestamp)
    mock_client.return_value.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=event_time,
            run=Run(
                runId=step_run_id,
                facets={
                    "parent": ParentRunFacet(
                        run={
                            "runId": pipeline_run_id
                        },
                        job={
                            "namespace": DEFAULT_NAMESPACE_NAME,
                            "name": pipeline_name
                        }
                    )
                }
            ),
            job=Job(
                namespace=DEFAULT_NAMESPACE_NAME,
                name=f"{pipeline_name}.{step_key}",
                facets={}
            ),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )
    )


@patch("openlineage.dagster.adapter.to_utc_iso_8601")
@patch("openlineage.dagster.adapter.OpenLineageClient")
def test_fail_step(mock_client, mock_to_utc_iso_8601):
    event_time = "2022-01-01T00:00:00.000000Z"
    mock_to_utc_iso_8601.return_value = event_time

    pipeline_name = "a_pipeline"
    pipeline_run_id = str(uuid.uuid4())
    step_run_id = str(uuid.uuid4())
    step_key = "an_op"
    timestamp = time.time()

    adapter = OpenLineageAdapter()
    adapter.fail_step(pipeline_name, pipeline_run_id, timestamp, step_run_id, step_key)

    mock_to_utc_iso_8601.assert_called_once_with(timestamp)
    mock_client.return_value.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.FAIL,
            eventTime=event_time,
            run=Run(
                runId=step_run_id,
                facets={
                    "parent": ParentRunFacet(
                        run={
                            "runId": pipeline_run_id
                        },
                        job={
                            "namespace": DEFAULT_NAMESPACE_NAME,
                            "name": pipeline_name
                        }
                    )
                }
            ),
            job=Job(
                namespace=DEFAULT_NAMESPACE_NAME,
                name=f"{pipeline_name}.{step_key}",
                facets={}
            ),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )
    )
