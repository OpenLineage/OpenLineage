# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for PrefectOpenLineageAdapter event creation and emission."""

import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from openlineage.client.run import RunEvent, RunState
from openlineage_prefect.prefect_adapter.adapter import (
    PRODUCER,
    DatasetInfo,
    DeploymentInfo,
    FlowInfo,
    PrefectOpenLineageAdapter,
    TaskInfo,
)


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.emit = MagicMock()
    return client


@pytest.fixture
def adapter(mock_client):
    return PrefectOpenLineageAdapter(client=mock_client)


@pytest.fixture
def sample_datetime():
    return datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")


@pytest.fixture
def sample_run_id():
    return str(uuid.uuid4())


@pytest.fixture
def deployment_info():
    return DeploymentInfo(
        id="dep-123",
        created=datetime.fromisoformat("2026-07-05T08:05:01.001+00:00"),
        updated=datetime.fromisoformat("2026-07-05T08:06:02.100+00:00"),
        name="test_deploy",
        namespace="default",
        flow_run=None,
    )


@pytest.fixture
def flow_info(sample_datetime):
    return FlowInfo(
        start_time=sample_datetime,
        name="test_flow",
        namespace="default",
        event_time=sample_datetime,
    )


@pytest.fixture
def task_info(sample_datetime, sample_run_id):
    return TaskInfo(
        run_id=sample_run_id,
        event_time=sample_datetime,
        name="test_task",
        namespace="default",
        event_type=RunState.START,
        expected_start_time=sample_datetime.isoformat(),
        prefect_version="3.7.6",
    )


def test_create_and_emit_flow_event(adapter, flow_info, deployment_info, sample_run_id):
    adapter.create_and_emit_flow_event(
        run_id=sample_run_id,
        event_type=RunState.START,
        flow_info=flow_info,
        prefect_version="3.7.6",
        deployment=deployment_info,
    )

    adapter.client.emit.assert_called_once()
    event = adapter.client.emit.call_args.args[0]
    assert isinstance(event, RunEvent)
    assert event.eventType == RunState.START
    assert event.eventTime == flow_info.event_time.isoformat()
    assert event.run.runId == sample_run_id
    assert event.job.name == "test_flow"
    assert event.job.namespace == "default"
    assert event.producer == PRODUCER
    assert event.run.facets["processingEngine"].version == "3.7.6"
    assert event.run.facets["processingEngine"].name == "Prefect"
    assert event.run.facets["prefectDeployment"].deployment_id == "dep-123"
    assert event.run.facets["prefectDeployment"].name == "test_deploy"
    assert event.job.facets["jobType"].processingType == "BATCH"
    assert event.job.facets["jobType"].integration == "Prefect"
    assert event.job.facets["jobType"].jobType == "FLOW"


def test_create_and_emit_flow_event_without_deployment(adapter, flow_info, sample_run_id):
    adapter.create_and_emit_flow_event(
        run_id=sample_run_id,
        event_type=RunState.COMPLETE,
        flow_info=flow_info,
    )

    event = adapter.client.emit.call_args.args[0]
    assert event.eventType == RunState.COMPLETE
    assert "prefectDeployment" not in event.run.facets
    assert event.run.facets["processingEngine"].version == "unknown"


def test_create_and_emit_flow_event_includes_error_message(adapter, sample_datetime, sample_run_id):
    flow_info = FlowInfo(
        start_time=sample_datetime,
        name="test_flow",
        namespace="default",
        event_time=sample_datetime,
        state_msg="flow failed",
    )
    adapter.create_and_emit_flow_event(
        run_id=sample_run_id,
        event_type=RunState.FAIL,
        flow_info=flow_info,
    )

    event = adapter.client.emit.call_args.args[0]
    assert event.run.facets["errorMessage"].message == "flow failed"


def test_create_and_emit_task_event(adapter, task_info, deployment_info):
    adapter.create_and_emit_task_event(
        task_info=task_info,
        flow_run_id="flow-run-456",
        flow_name="test_flow",
        deployment=deployment_info,
        datasets=DatasetInfo(input_datasets=[], output_datasets=[]),
    )

    adapter.client.emit.assert_called_once()
    event = adapter.client.emit.call_args.args[0]
    assert isinstance(event, RunEvent)
    assert event.eventType == RunState.START
    assert event.eventTime == task_info.event_time.isoformat()
    assert event.job.name == "test_task"
    assert event.job.namespace == "default"
    assert event.run.facets["nominalTime"].nominalStartTime == task_info.expected_start_time
    assert event.run.facets["parentRun"].run["runId"] == "flow-run-456"
    assert event.run.facets["parentRun"].job["name"] == "test_flow"
    assert event.run.facets["prefectDeployment"].deployment_id == "dep-123"
    assert event.run.facets["processingEngine"].version == "3.7.6"
    assert event.run.facets["processingEngine"].name == "Prefect"
    assert event.job.facets["jobType"].processingType == "BATCH"
    assert event.job.facets["jobType"].integration == "Prefect"
    assert event.job.facets["jobType"].jobType == "TASK"


def test_create_and_emit_task_complete_event(adapter, task_info):
    task_info.event_type = RunState.COMPLETE
    adapter.create_and_emit_task_event(task_info=task_info)

    event = adapter.client.emit.call_args.args[0]
    assert event.eventType == RunState.COMPLETE


def test_create_and_emit_task_event_includes_job_dependencies(adapter, task_info):
    task_info.job_deps = [
        {"namespace": "default", "name": "parent_task_1"},
        {"namespace": "default", "name": "parent_task_2"},
    ]
    adapter.create_and_emit_task_event(task_info=task_info)

    upstream = adapter.client.emit.call_args.args[0].run.facets["jobDependencies"].upstream
    assert [item.job.name for item in upstream] == ["parent_task_1", "parent_task_2"]
    assert upstream[0].job.namespace == "default"


def test_create_and_emit_task_event_omits_empty_job_dependencies(adapter, task_info):
    adapter.create_and_emit_task_event(task_info=task_info)

    event = adapter.client.emit.call_args.args[0]
    assert "jobDependencies" not in event.run.facets
    assert event.run.facets["processingEngine"].version == "3.7.6"


def test_create_and_emit_task_event_includes_datasets(adapter, task_info):
    datasets = DatasetInfo(
        input_datasets=[
            {"uri": "postgres://localhost", "table": "users"},
            {"uri": "postgres://localhost", "table": "accounts"},
        ],
        output_datasets=[{"uri": "postgres://localhost", "table": "results"}],
    )
    adapter.create_and_emit_task_event(task_info=task_info, datasets=datasets)

    event = adapter.client.emit.call_args.args[0]
    assert [(dataset.namespace, dataset.name) for dataset in event.inputs] == [
        ("postgres://localhost", "users"),
        ("postgres://localhost", "accounts"),
    ]
    assert [(dataset.namespace, dataset.name) for dataset in event.outputs] == [
        ("postgres://localhost", "results")
    ]


def test_create_and_emit_task_event_includes_error_message(adapter, task_info):
    task_info.event_type = RunState.FAIL
    task_info.state_msg = "task failed"
    adapter.create_and_emit_task_event(task_info=task_info)

    event = adapter.client.emit.call_args.args[0]
    assert event.eventType == RunState.FAIL
    assert event.run.facets["errorMessageRun"].message == "task failed"


def test_adapter_initialization_with_client(mock_client):
    assert PrefectOpenLineageAdapter(client=mock_client).client is mock_client


def test_producer_constant():
    assert PRODUCER == "https://github.com/OpenLineage/openlineage/integration/prefect"
