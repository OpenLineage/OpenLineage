# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for PrefectOpenLineageListener module.

Tests cover event processing, Prefect API interactions, and OpenLineage event creation.
"""

import json
import logging
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from openlineage.client.run import RunState
from openlineage_prefect.prefect_adapter.adapter import (
    DeploymentInfo,
    FlowInfo,
    PrefectOpenLineageAdapter,
)
from openlineage_prefect.prefect_adapter.listener import PrefectOpenLineageListener
from prefect.events.schemas.events import Event
from test_events import FLOW_START_EVENT, TASK_START_EVENT

FLOW_NAME_TYPE = "flow-run"
# ========== Fixtures ==========


@pytest.fixture
def mock_prefect_client():
    """Create a mock Prefect API client."""
    return AsyncMock()


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
    return PrefectOpenLineageListener(client=mock_prefect_client, ol_adapter=mock_adapter)


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
        prefect_run_id="prefect-run-id",
    )
    assert isinstance(run_id, str)
    assert len(run_id) > 0


def test_build_run_id_deterministic(listener):
    """Test that build_run_id is deterministic (same inputs produce same output)."""
    execution_time = datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")
    run_name = "test_flow"
    namespace = "default"

    prefect_run_id = "prefect-run-id"
    run_id_1 = listener.build_run_id(execution_time, run_name, namespace, prefect_run_id)
    run_id_2 = listener.build_run_id(execution_time, run_name, namespace, prefect_run_id)

    assert run_id_1 == run_id_2


def test_build_run_id_different_for_different_inputs(listener):
    """Test that different inputs produce different UUIDs."""
    execution_time = datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")

    prefect_run_id = "prefect-run-id"
    run_id_1 = listener.build_run_id(execution_time, "flow_a", "default", prefect_run_id)
    run_id_2 = listener.build_run_id(execution_time, "flow_b", "default", prefect_run_id)

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

    assert isinstance(result, DeploymentInfo)
    assert (
        DeploymentInfo(
            id="dep-123",
            created=datetime.fromisoformat("2026-07-05T08:05:01.001+00:00"),
            updated=datetime.fromisoformat("2026-07-05T08:06:02.100+00:00"),
            name="test_deploy",
            namespace="custom_namespace",
            flow_run=flow_run,
        )
        == result
    )


# ========== Tests for get_flow_info ==========


@pytest.mark.asyncio
async def test_get_flow_info_success(listener, mock_prefect_client):
    """Test successful retrieval of flow info."""
    flow_run = MagicMock()
    flow_run.start_time = datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")
    flow_run.flow_id = "flow-456"
    flow_run.name = "test_flow_run"
    flow = MagicMock()
    flow.name = "test_flow"
    mock_prefect_client.read_flow.return_value = flow

    with patch("openlineage_prefect.prefect_adapter.listener.FLOW_NAME_TYPE", "flow"):
        flow_info = await listener.get_flow_info(flow_run)

    assert isinstance(flow_info, FlowInfo)
    assert flow_info.start_time == datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")
    assert flow_info.name == "test_flow"


# ========== Tests for get_prefect_version ==========


@pytest.mark.asyncio
async def test_get_prefect_version_success(listener, mock_prefect_client):
    """Test successful version retrieval."""
    mock_prefect_client.api_version.return_value = "3.7.6"

    version = await listener.get_prefect_version()

    assert version == "3.7.6"
    mock_prefect_client.api_version.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_get_prefect_version_handles_error(listener, mock_prefect_client):
    """Test that version retrieval handles errors gracefully."""
    mock_prefect_client.api_version.side_effect = httpx.ConnectError("API error")

    version = await listener.get_prefect_version()

    assert version is None


@pytest.mark.asyncio
async def test_get_prefect_version_does_not_suppress_unexpected_error(listener, mock_prefect_client):
    """Test that version retrieval only handles HTTP and JSON decoding errors."""
    mock_prefect_client.api_version.side_effect = TypeError("Unexpected error")

    with pytest.raises(TypeError, match="Unexpected error"):
        await listener.get_prefect_version()


@pytest.mark.asyncio
async def test_get_prefect_version_logs_error(listener, mock_prefect_client, caplog):
    """Test that version retrieval logs the caught error."""
    caplog.set_level(logging.INFO)
    mock_prefect_client.api_version.side_effect = httpx.ConnectError("API error")

    version = await listener.get_prefect_version()

    assert version is None
    assert "ConnectError: API error" in caplog.text


def test_process_artifact_description_logs_errors(listener, caplog):
    """Test that malformed artifact descriptions include exception details in logs."""

    class InvalidDescription(str):
        def split(self):
            raise ValueError

    caplog.set_level(logging.WARNING)

    dataset_type = listener.process_artifact_description(
        {"description": InvalidDescription("ol-dataset_input")}
    )

    assert dataset_type is None
    assert "ValueError: Cannot split on _" in caplog.text
    assert "ValueError: Cannot split on -" in caplog.text


@pytest.mark.asyncio
async def test_get_deployment_info_uses_default_namespace(listener, mock_prefect_client):
    """Test deployment namespace fallback when no deployment override exists."""
    flow_run = MagicMock()
    flow_run.deployment_id = "dep-123"
    deployment = MagicMock()
    deployment.job_variables = {}

    mock_prefect_client.read_flow_run.return_value = flow_run
    mock_prefect_client.read_deployment.return_value = deployment

    result = await listener.get_deployment_info("flow-run-123")

    assert result.namespace == "default"


# ========== Tests for get_artifacts_by_task_run ==========


@pytest.mark.asyncio
async def test_get_artifacts_by_task_run_success(listener, mock_prefect_client):
    """Test successful artifact retrieval and parsing."""
    artifacts = [
        {
            "description": "ol-dataset_input",
            "data": json.dumps([{"database_uri": "postgres://localhost", "table": "users"}]),
        },
        {
            "description": "ol-dataset_output",
            "data": json.dumps([{"database_uri": "postgres://localhost", "table": "results"}]),
        },
    ]

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = artifacts

    mock_prefect_client._client.post.return_value = mock_response  # NOQA: SLF001

    result = await listener.get_artifacts_by_task_run("task-run-123")

    assert result[0]["uri"] == "postgres://localhost"
    assert result[0]["table"] == "users"
    assert result[0]["dataset_type"] == "input"
    assert result[1]["dataset_type"] == "output"
    mock_prefect_client._client.post.assert_awaited_once_with(  # NOQA: SLF001
        "/artifacts/filter",
        json={"artifacts": {"task_run_id": {"any_": ["task-run-123"]}}},
    )


@pytest.mark.asyncio
async def test_get_artifacts_by_task_run_filters_ol_datasets(listener, mock_prefect_client):
    """Test that non-openlineage artifacts are filtered out."""
    artifacts = [
        {
            "description": "regular artifact",
            "data": json.dumps([{"database_uri": "postgres://localhost", "table": "users"}]),
        },
        {
            "description": "ol-dataset_input",
            "data": json.dumps([{"database_uri": "postgres://localhost", "table": "results"}]),
        },
    ]

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = artifacts

    mock_prefect_client._client.post.return_value = mock_response  # NOQA: SLF001

    result = await listener.get_artifacts_by_task_run("task-run-123")

    assert len(result) == 1
    assert result[0]["uri"] == "postgres://localhost"


@pytest.mark.asyncio
async def test_get_artifacts_by_task_run_no_artifacts(listener, mock_prefect_client):
    """Test handling when no artifacts are found."""
    mock_response = MagicMock()
    mock_response.status_code = 404

    mock_prefect_client._client.post.return_value = mock_response  # NOQA: SLF001

    result = await listener.get_artifacts_by_task_run("task-run-123")

    assert result == []


@pytest.mark.asyncio
async def test_get_artifacts_by_task_run_empty_list(listener, mock_prefect_client):
    """Test handling when artifacts list is empty."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []

    mock_prefect_client._client.post.return_value = mock_response  # NOQA: SLF001

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
    parent_task_run.start_time = datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")
    parent_task_run.flow_run_id = "flow-run-456"

    mock_prefect_client.read_task_run.return_value = parent_task_run

    with patch.object(
        listener,
        "get_deployment_info",
        new_callable=AsyncMock,
        return_value=DeploymentInfo(namespace="default", flow_run=None),
    ):
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
    parent_task_run.start_time = datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00")
    parent_task_run.flow_run_id = "flow-run-456"

    mock_prefect_client.read_task_run.return_value = parent_task_run

    with patch.object(
        listener,
        "get_deployment_info",
        new_callable=AsyncMock,
        return_value=DeploymentInfo(namespace="default", flow_run=None),
    ):
        result = await listener.get_parent_runs(payload, "task-run-123")

    assert [run["name"] for run in result] == ["parent_task", "parent_task"]


# ========== Tests for collect_and_process_flow_runs ==========


@pytest.mark.asyncio
async def test_collect_and_process_flow_runs_success(listener, sample_flow_event, mock_adapter):
    """Test successful flow event processing."""
    flow_run = MagicMock()
    deployment = DeploymentInfo(namespace="default", flow_run=flow_run)
    flow_info = FlowInfo(
        start_time=datetime.fromisoformat("2026-07-06T11:04:46.467291+00:00"),
        name="GitHub Stars",
        namespace=None,
        event_time=None,
    )

    with (
        patch.object(listener, "get_deployment_info", new_callable=AsyncMock, return_value=deployment),
        patch.object(listener, "get_flow_info", new_callable=AsyncMock, return_value=flow_info),
    ):
        await listener.collect_and_process_flow_runs("3.7.6", sample_flow_event, RunState.START)

    mock_adapter.create_and_emit_flow_event.assert_called_once()
    call_args = mock_adapter.create_and_emit_flow_event.call_args.kwargs
    assert call_args["event_type"] == RunState.START
    assert call_args["flow_info"].name == "GitHub Stars"
    assert call_args["flow_info"].namespace == "default"
    assert call_args["prefect_version"] == "3.7.6"


# ========== Tests for collect_and_process_task_runs ==========


@pytest.mark.asyncio
async def test_collect_and_process_task_runs_success(listener, sample_task_event, mock_adapter):
    """Test successful task event processing."""
    deployment = DeploymentInfo(namespace="default", flow_run=MagicMock())
    flow_info = FlowInfo(
        start_time=datetime.fromisoformat("2026-07-07T13:21:07.336123+00:00"),
        name="GitHub Stars",
        namespace=None,
        event_time=None,
    )

    with (
        patch.object(listener, "get_deployment_info", new_callable=AsyncMock, return_value=deployment),
        patch.object(listener, "build_run_id", return_value="task-run-id"),
        patch.object(listener, "get_artifacts_by_task_run", new_callable=AsyncMock, return_value=[]),
        patch.object(listener, "get_parent_runs", new_callable=AsyncMock, return_value=[]),
        patch.object(listener, "get_flow_info", new_callable=AsyncMock, return_value=flow_info),
    ):
        await listener.collect_and_process_task_runs("3.7.6", sample_task_event, RunState.START)

    mock_adapter.create_and_emit_task_event.assert_called_once()
    call_args = mock_adapter.create_and_emit_task_event.call_args.kwargs
    assert call_args["task_info"].event_type == RunState.START
    assert call_args["task_info"].name == "fake_ingest"
    assert call_args["task_info"].namespace == "default"
    assert (
        call_args["task_info"].expected_start_time
        == sample_task_event.payload["task_run"]["expected_start_time"]
    )
    assert call_args["flow_name"] == "GitHub Stars"


@pytest.mark.asyncio
async def test_collect_and_process_task_runs_with_datasets(listener, sample_task_event, mock_adapter):
    """Test task processing with input and output datasets."""
    mock_adapter.create_and_emit_task_event = MagicMock()

    task_run = MagicMock()
    task_run.start_time = datetime.fromisoformat("2026-07-07T13:21:07.336123+00:00")

    datasets = [
        {"uri": "postgres://localhost", "table": "users", "dataset_type": "input"},
        {"uri": "postgres://localhost", "table": "results", "dataset_type": "output"},
    ]
    deployment = DeploymentInfo(namespace="default", flow_run=MagicMock())
    flow_info = FlowInfo(
        start_time=datetime.fromisoformat("2026-07-07T13:21:07.336123+00:00"),
        name="GitHub Stars",
        namespace=None,
        event_time=None,
    )

    with (
        patch.object(listener, "get_deployment_info", new_callable=AsyncMock, return_value=deployment),
        patch.object(listener, "build_run_id", return_value="task-run-id"),
        patch.object(listener, "get_artifacts_by_task_run", new_callable=AsyncMock, return_value=datasets),
        patch.object(listener, "get_parent_runs", new_callable=AsyncMock, return_value=[]),
        patch.object(listener, "get_flow_info", new_callable=AsyncMock, return_value=flow_info),
    ):
        await listener.collect_and_process_task_runs("3.7.6", sample_task_event, RunState.START)

    call_args = mock_adapter.create_and_emit_task_event.call_args
    dataset_info = call_args.kwargs["datasets"]
    assert len(dataset_info.input_datasets) == 1
    assert len(dataset_info.output_datasets) == 1
    assert dataset_info.input_datasets[0]["uri"] == "postgres://localhost"


# ========== Event state mapping tests ==========


def test_listener_initialization(listener, mock_prefect_client, mock_adapter):
    """Test listener initialization with custom client and adapter."""
    assert listener.client == mock_prefect_client
    assert listener.ol_adapter == mock_adapter
