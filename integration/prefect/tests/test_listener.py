# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for PrefectOpenLineageListener module.

Tests cover event processing, Prefect API interactions, and OpenLineage event creation.
"""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from prefect_adapter.adapter import PrefectOpenLineageAdapter
from prefect_adapter.listener import PrefectOpenLineageListener
from prefect.events.schemas.events import Event
from test_events import FLOW_START_EVENT, TASK_START_EVENT

FLOW_NAME_TYPE = "flow-run"
# ========== Fixtures ==========


@pytest.fixture
def mock_prefect_client():
    """Create a mock Prefect API client."""
    client = AsyncMock()
    client._client = AsyncMock()
    return client


@pytest.fixture
def mock_adapter():
    """Create a mock OpenLineage adapter."""
    adapter = MagicMock(spec=PrefectOpenLineageAdapter)
    adapter.create_and_emit_flow_event = MagicMock()
    adapter.create_and_emit_task_event = MagicMock()
    return adapter


@pytest.fixture
def listener(mock_prefect_client, mock_adapter):
    """Create a listener instance with mocked dependencies."""
    return PrefectOpenLineageListener(client=mock_prefect_client, adapter=mock_adapter)


@pytest.fixture
def sample_flow_event():
    """Create a sample flow start event."""
    return Event(**json.loads(FLOW_START_EVENT))


@pytest.fixture
def sample_task_event():
    """Create a sample task start event."""
    return Event(**json.loads(TASK_START_EVENT))


# ========== Tests for build_run_id ==========


def test_build_run_id_returns_string(listener):
    """Test that build_run_id returns a string UUID."""
    run_id = listener.build_run_id(
        execution_time=datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00"),
        run_name="test_flow",
        namespace="default",
    )
    assert isinstance(run_id, str)
    assert len(run_id) > 0


def test_build_run_id_deterministic(listener):
    """Test that build_run_id is deterministic (same inputs produce same output)."""
    execution_time = datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")
    run_name = "test_flow"
    namespace = "default"

    run_id_1 = listener.build_run_id(execution_time, run_name, namespace)
    run_id_2 = listener.build_run_id(execution_time, run_name, namespace)

    assert run_id_1 == run_id_2


def test_build_run_id_different_for_different_inputs(listener):
    """Test that different inputs produce different UUIDs."""
    execution_time = datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")

    run_id_1 = listener.build_run_id(execution_time, "flow_a", "default")
    run_id_2 = listener.build_run_id(execution_time, "flow_b", "default")

    assert run_id_1 != run_id_2


# ========== Tests for get_deployment_info ==========


@pytest.mark.asyncio
async def test_get_deployment_info_success(listener, mock_prefect_client):
    """Test successful retrieval of deployment and flow info."""
    # Setup mock responses
    flow_run = MagicMock()
    flow_run.deployment_id = "dep-123"
    flow_run.flow_id = "flow-456"

    deployment = MagicMock()
    deployment.id = "dep-123"
    deployment.created = datetime.fromisoformat("2026-07-05T08:05:01.001+00:00")
    deployment.updated = datetime.fromisoformat("2026-07-05T08:06:02.100+00:00")
    deployment.name = "test_deploy"
    deployment.job_variables = {"env": {"OPENLINEAGE_NAMESPACE": "custom_namespace"}}

    flow = MagicMock()
    flow.name = "test_flow"

    mock_prefect_client.read_flow_run.return_value = flow_run
    mock_prefect_client.read_deployment.return_value = deployment
    mock_prefect_client.read_flow.return_value = flow

    result = await listener.get_deployment_info("flow-run-123")

    assert isinstance(result, PrefectOpenLineageListener.DeploymentInfo)
    assert PrefectOpenLineageListener.DeploymentInfo(
        id="dep-123",
        created=datetime.fromisoformat("2026-07-05T08:05:01.001+00:00"),
        updated=datetime.fromisoformat("2026-07-05T08:06:02.100+00:00"),
        name="test_deploy",
        namespace="custom_namespace",
        flow_run=flow_run,
    ) == result

# ========== Tests for get_flow_info ==========

@pytest.mark.asyncio
async def test_get_flow_info_success(listener, mock_prefect_client):
    """Test successful retrieval of flow info."""
    flow_run = MagicMock()
    flow_run.start_time = datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")
    mock_prefect_client.read_flow_run.return_value = flow_run
 
    flow_info = await listener.get_flow_info(flow_run)

    assert isinstance(flow_info, PrefectOpenLineageListener.FlowInfo)
    assert flow_info.start_time == datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")


# ========== Tests for get_prefect_version ==========


@pytest.mark.asyncio
async def test_get_prefect_version_success(listener, mock_prefect_client):
    """Test successful version retrieval."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"version": "3.7.6"}
    mock_prefect_client._client.get.return_value = mock_response

    version = await listener.get_prefect_version()

    assert version == {"version": "3.7.6"}
    mock_prefect_client._client.get.assert_called_once_with("/admin/version")


@pytest.mark.asyncio
async def test_get_prefect_version_handles_error(listener, mock_prefect_client):
    """Test that version retrieval handles errors gracefully."""
    mock_prefect_client._client.get.side_effect = TypeError("API error")

    version = await listener.get_prefect_version()

    assert version is None


# ========== Tests for get_flow_ns ==========


@pytest.mark.asyncio
async def test_get_flow_ns_from_deployment_variables(listener, mock_prefect_client):
    """Test namespace retrieval from deployment job variables."""
    flow_run = MagicMock()
    flow_run.deployment_id = "dep-123"

    deployment = MagicMock()
    deployment.job_variables = {"env": {"OPENLINEAGE_NAMESPACE": "custom_ns"}}

    mock_prefect_client.read_flow_run.return_value = flow_run
    mock_prefect_client.read_deployment.return_value = deployment

    ns = await listener.get_flow_ns("flow-run-123")

    assert ns == "custom_ns"


# ========== Tests for get_artifacts_by_task_run ==========


@pytest.mark.asyncio
async def test_get_artifacts_by_task_run_success(listener, mock_prefect_client):
    """Test successful artifact retrieval and parsing."""
    artifacts = [
        {
            "description": "ol-dataset_input",
            "data": str([{"database_uri": "postgres://localhost", "table": "users"}]),
        },
        {
            "description": "ol-dataset_output",
            "data": str([{"database_uri": "postgres://localhost", "table": "results"}]),
        },
    ]

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = artifacts

    mock_prefect_client._client.post.return_value = mock_response

    result = await listener.get_artifacts_by_task_run("task-run-123")

    assert len(result) == 2
    assert result[0]["uri"] == "postgres://localhost"
    assert result[0]["table"] == "users"
    assert result[0]["dataset_type"] == "input"
    assert result[1]["dataset_type"] == "output"


@pytest.mark.asyncio
async def test_get_artifacts_by_task_run_filters_ol_datasets(
    listener, mock_prefect_client
):
    """Test that non-openlineage artifacts are filtered out."""
    artifacts = [
        {
            "description": "regular artifact",
            "data": str([{"database_uri": "postgres://localhost", "table": "users"}]),
        },
        {
            "description": "ol-dataset_input",
            "data": str([{"database_uri": "postgres://localhost", "table": "results"}]),
        },
    ]

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = artifacts

    mock_prefect_client._client.post.return_value = mock_response

    result = await listener.get_artifacts_by_task_run("task-run-123")

    assert len(result) == 1
    assert result[0]["uri"] == "postgres://localhost"


@pytest.mark.asyncio
async def test_get_artifacts_by_task_run_no_artifacts(listener, mock_prefect_client):
    """Test handling when no artifacts are found."""
    mock_response = MagicMock()
    mock_response.status_code = 404

    mock_prefect_client._client.post.return_value = mock_response

    result = await listener.get_artifacts_by_task_run("task-run-123")

    assert result == []


@pytest.mark.asyncio
async def test_get_artifacts_by_task_run_empty_list(listener, mock_prefect_client):
    """Test handling when artifacts list is empty."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []

    mock_prefect_client._client.post.return_value = mock_response

    result = await listener.get_artifacts_by_task_run("task-run-123")

    assert result == []


# ========== Tests for get_parent_runs ==========


@pytest.mark.asyncio
async def test_get_parent_runs_success(listener, mock_prefect_client):
    """Test successful retrieval of parent task runs."""
    payload = {
        "task_run": {
            "task_inputs": {
                "__parents__": [
                    {"input_type": "task_run", "id": "parent-task-123"},
                    {"input_type": "parameter", "id": "param-456"},
                ]
            }
        }
    }

    parent_task_run = MagicMock()
    parent_task_run.name = "parent_task-abc"
    parent_task_run.start_time = datetime.fromisoformat(
        "2026-07-06T11:04:46.467291+00:00"
    )

    mock_prefect_client.read_task_run.return_value = parent_task_run

    with patch.object(listener, "get_flow_ns", return_value="default"):
        result = await listener.get_parent_runs(payload, "task-run-123")

    assert len(result) == 1
    assert result[0]["name"] == "parent_task"
    assert result[0]["namespace"] == "default"
    assert isinstance(result[0]["id"], str)


@pytest.mark.asyncio
async def test_get_parent_runs_no_parents(listener):
    """Test handling when task has no parent dependencies."""
    payload = {"task_run": {"task_inputs": {"__parents__": []}}}

    result = await listener.get_parent_runs(payload, "task-run-123")

    assert result == []


@pytest.mark.asyncio
async def test_get_parent_runs_missing_key(listener):
    """Test handling when __parents__ key is missing."""
    payload = {"task_run": {"task_inputs": {}}}

    result = await listener.get_parent_runs(payload, "task-run-123")

    assert result == []


@pytest.mark.asyncio
async def test_get_parent_runs_filters_non_task_inputs(listener, mock_prefect_client):
    """Test that non-task parent inputs are filtered out."""
    payload = {
        "task_run": {
            "task_inputs": {
                "__parents__": [
                    {"input_type": "task_run", "id": "parent-task-123"},
                    {"input_type": "task_run", "id": "parent-task-456"},
                    {"input_type": "parameter", "id": "param-789"},
                ]
            }
        }
    }

    parent_task_run = MagicMock()
    parent_task_run.name = "parent_task-abc"
    parent_task_run.start_time = datetime.fromisoformat(
        "2026-07-06T11:04:46.467291+00:00"
    )

    mock_prefect_client.read_task_run.return_value = parent_task_run

    with patch.object(listener, "get_flow_ns", return_value="default"):
        result = await listener.get_parent_runs(payload, "task-run-123")

    assert len(result) == 2


# ========== Tests for collect_and_process_flow_runs ==========


@pytest.mark.asyncio
async def test_collect_and_process_flow_runs_success(
    listener, sample_flow_event, mock_adapter
):
    """Test successful flow event processing."""
    mock_adapter.create_and_emit_flow_event = MagicMock()

    with patch.object(listener, "get_flow_info") as mock_get_info:
        mock_get_info.return_value = PrefectOpenLineageListener.FlowInfo(
            datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00"),  # start_time
            "test_flow",  # flow_name
        )

        prefect_version = "3.7.6"
        event_state = "START"

        await listener.collect_and_process_flow_runs(
            prefect_version, sample_flow_event, event_state
        )

    # The function loops through related items and processes each, so it may be called multiple times
    assert mock_adapter.create_and_emit_flow_event.call_count > 0
    call_args = mock_adapter.create_and_emit_flow_event.call_args_list[0]
    assert call_args.kwargs["event_type"] == "START"
    assert call_args.kwargs["flow_name"] == "GitHub Stars"


# ========== Tests for collect_and_process_task_runs ==========


@pytest.mark.asyncio
async def test_collect_and_process_task_runs_success(
    listener, sample_task_event, mock_adapter
):
    """Test successful task event processing."""
    mock_adapter.create_and_emit_task_event = MagicMock()

    task_run = MagicMock()
    task_run.start_time = datetime.fromisoformat("2026-07-07T13:21:07.336123+00:00")

    with (
        patch.object(listener, "get_flow_ns", return_value="default"),
        patch.object(listener, "build_run_id", return_value="task-run-id"),
        patch.object(listener, "get_artifacts_by_task_run", return_value=[]),
        patch.object(listener, "get_parent_runs", return_value=[]),
        patch.object(listener, "get_flow_info") as mock_get_info,
    ):
        mock_prefect_client = listener.client
        mock_prefect_client.read_task_run.return_value = task_run

        mock_get_info.return_value = PrefectOpenLineageListener.FlowInfo(
            datetime.fromisoformat("2026-07-07T13:21:07.336123+00:00"),
            "GitHub Stars",
        )

        await listener.collect_and_process_task_runs(
            "3.7.6", sample_task_event, "START"
        )

    mock_adapter.create_and_emit_task_event.assert_called_once()
    call_args = mock_adapter.create_and_emit_task_event.call_args
    assert call_args.kwargs["event_type"] == "START"
    assert call_args.kwargs["task_name"] == "fake_ingest"


@pytest.mark.asyncio
async def test_collect_and_process_task_runs_with_datasets(
    listener, sample_task_event, mock_adapter
):
    """Test task processing with input and output datasets."""
    mock_adapter.create_and_emit_task_event = MagicMock()

    task_run = MagicMock()
    task_run.start_time = datetime.fromisoformat("2026-07-07T13:21:07.336123+00:00")

    datasets = [
        {"uri": "postgres://localhost", "table": "users", "dataset_type": "input"},
        {"uri": "postgres://localhost", "table": "results", "dataset_type": "output"},
    ]

    with (
        patch.object(listener, "get_flow_ns", return_value="default"),
        patch.object(listener, "build_run_id", return_value="task-run-id"),
        patch.object(listener, "get_artifacts_by_task_run", return_value=datasets),
        patch.object(listener, "get_parent_runs", return_value=[]),
        patch.object(listener, "get_flow_info") as mock_get_info,
    ):
        mock_prefect_client = listener.client
        mock_prefect_client.read_task_run.return_value = task_run

        mock_get_info.return_value = PrefectOpenLineageListener.FlowInfo(
            "dep-123",
            datetime.fromisoformat("2026-07-07T13:21:07.336123+00:00"),
        )

        await listener.collect_and_process_task_runs(
            "3.7.6", sample_task_event, "START"
        )

    call_args = mock_adapter.create_and_emit_task_event.call_args
    assert len(call_args.kwargs["input_datasets"]) == 1
    assert len(call_args.kwargs["output_datasets"]) == 1
    assert call_args.kwargs["input_datasets"][0]["uri"] == "postgres://localhost"


# ========== Event state mapping tests ==========


def test_listener_initialization(listener, mock_prefect_client, mock_adapter):
    """Test listener initialization with custom client and adapter."""
    assert listener.client == mock_prefect_client
    assert listener.adapter == mock_adapter
