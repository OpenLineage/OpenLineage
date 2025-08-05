# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Generator, Union
from unittest.mock import MagicMock, patch

import httpx
import pytest
from openlineage.client import event_v2, set_producer
from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
from openlineage.client.transport.async_http import AsyncHttpTransport
from openlineage.client.transport.http import HttpConfig, HttpTransport
from openlineage.client.transport.transport import Config, Transport


@pytest.fixture(scope="session")
def test_producer():
    return "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python"


@pytest.fixture(scope="session")
def root() -> Path:
    return Path(__file__).parent


@pytest.fixture(scope="session", autouse=True)
def _setup_producer(test_producer) -> None:
    set_producer(test_producer)


# For testing events emitted by the client

Event_v1 = Union[RunEvent, DatasetEvent, JobEvent]
Event_v2 = Union[event_v2.RunEvent, event_v2.DatasetEvent, event_v2.JobEvent]
Event = Union[Event_v1, Event_v2]


class NoOutputConfig(Config):
    ...


class NoOutputTransport(Transport):
    """Minimal transport that stores the event for assertions"""

    kind = "no_output"
    config_class = NoOutputConfig

    def __init__(self, config: NoOutputConfig) -> None:  # noqa: ARG002
        self._event = None

    def emit(self, event: Event) -> None:
        self._event = event

    @property
    def event(self):
        return self._event


@pytest.fixture(scope="session")
def transport():
    return NoOutputTransport(config=NoOutputConfig())


# Integration test fixtures for HTTP transport testing


@pytest.fixture(scope="session")
def docker_compose_file():
    """Path to docker-compose file for integration tests."""
    return "tests/integration/docker-compose.yml"


@pytest.fixture(scope="session")
def test_server_url():
    """URL for the test server."""
    return "http://localhost:8081"


@pytest.fixture(scope="session")
def test_server(docker_compose_file: str, test_server_url: str) -> Generator[str, None, None]:
    """Start test server using Docker Compose and yield the URL."""
    # Start the test server
    print(f"Starting test server with docker-compose file: {docker_compose_file}")
    try:
        result = subprocess.run(
            ["docker-compose", "-f", docker_compose_file, "up", "-d", "--build"],
            check=True,
            cwd=".",
            capture_output=True,
            text=True,
            timeout=40,
        )
        print(f"Docker compose up stdout: {result.stdout}")
        print(f"Docker compose up stderr: {result.stderr}")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        # print(f"Docker compose up failed with exit code {e.returncode}")
        print("stdout:")
        for x in e.stdout.splitlines():
            print(x)
        print("stderr:")
        for y in e.stderr.splitlines():
            print(y)
        raise

    # Wait for server to be healthy
    max_retries = 30
    retry_delay = 1.0

    for attempt in range(max_retries):
        try:
            response = httpx.get(f"{test_server_url}/health", timeout=5.0)
            if response.status_code == 200:
                print(f"Test server is ready after {attempt + 1} attempts")
                break
        except (httpx.RequestError, httpx.TimeoutException):
            if attempt == max_retries - 1:
                # Cleanup on failure
                print("Test server failed to start, cleaning up...")
                cleanup_result = subprocess.run(
                    ["docker-compose", "-f", docker_compose_file, "down"],
                    cwd=".",
                    capture_output=True,
                    text=True,
                )
                print(f"Cleanup stdout: {cleanup_result.stdout}")
                print(f"Cleanup stderr: {cleanup_result.stderr}")
                raise RuntimeError(f"Test server failed to start after {max_retries} attempts")
            time.sleep(retry_delay)

    try:
        yield test_server_url
    finally:
        # Cleanup
        print("Cleaning up test server...")
        cleanup_result = subprocess.run(
            ["docker-compose", "-f", docker_compose_file, "down"], cwd=".", capture_output=True, text=True
        )
        print(f"Final cleanup stdout: {cleanup_result.stdout}")
        print(f"Final cleanup stderr: {cleanup_result.stderr}")


@pytest.fixture(autouse=True)
def reset_test_server(request):
    """Reset test server state before and after each test (including parameterized tests)."""
    # Only run for HTTP integration tests
    test_file = str(request.node.fspath) if hasattr(request.node, "fspath") else ""
    if "test_http_integration" not in test_file:
        yield
        return

    test_server_url = "http://localhost:8081"

    # Reset before test
    try:
        response = httpx.post(f"{test_server_url}/reset", timeout=5.0)
        response.raise_for_status()
        print(f"✓ Reset test server before {request.node.name}")

        # Wait a brief moment for reset to complete
        import time

        time.sleep(0.1)

        # Verify reset worked
        stats_response = httpx.get(f"{test_server_url}/stats", timeout=5.0)
        stats_response.raise_for_status()
        stats = stats_response.json()
        if stats["total_events"] > 0:
            print(f"Warning: Test server still has {stats['total_events']} events after reset")
    except Exception as e:
        print(f"Warning: Failed to reset test server before: {e}")

    yield

    # Reset after test (for parameterized tests, this runs after each parameter)
    try:
        response = httpx.post(f"{test_server_url}/reset", timeout=5.0)
        response.raise_for_status()
        print(f"✓ Reset test server after {request.node.name}")
    except Exception as e:
        print(f"Warning: Failed to reset test server after: {e}")


@pytest.fixture
def sync_transport(test_server: str) -> HttpTransport:
    """Create a synchronous HTTP transport configured for the test server."""
    config = HttpConfig(
        url=test_server,
        timeout=5.0,
        verify=False,  # No SSL in test server
    )
    return HttpTransport(config)


@pytest.fixture
def async_transport(test_server: str) -> Generator[AsyncHttpTransport]:
    """Create an asynchronous HTTP transport configured for the test server."""
    from openlineage.client.transport.async_http import AsyncHttpConfig

    config = AsyncHttpConfig(
        url=test_server,
        timeout=5.0,
        verify=False,  # No SSL in test server
        max_queue_size=1000,
        max_concurrent_requests=10,
    )
    transport = AsyncHttpTransport(config)

    yield transport

    # Cleanup
    transport.close(timeout=10.0)


@pytest.fixture
def async_transport_custom(test_server: str):
    """Factory for creating async transports with custom configuration."""

    def _create_transport(
        max_queue_size: int = 100,
        max_concurrent_requests: int = 5,
        timeout: float = 5.0,
        retry_count: int = 5,
        backoff_factor: float = 0.3,
    ) -> AsyncHttpTransport:
        from openlineage.client.transport.async_http import AsyncHttpConfig

        config = AsyncHttpConfig(
            url=test_server,
            timeout=timeout,
            verify=False,
            retry={"total": retry_count, "backoff_factor": backoff_factor},
            max_queue_size=max_queue_size,
            max_concurrent_requests=max_concurrent_requests,
        )
        return AsyncHttpTransport(config)

    return _create_transport


@pytest.fixture
def test_server_client(test_server: str) -> httpx.Client:
    """HTTP client for interacting with test server endpoints."""
    with httpx.Client(base_url=test_server, timeout=10.0) as client:
        yield client


class TestServerHelper:
    """Helper class for interacting with the test server."""

    def __init__(self, client: httpx.Client):
        self.client = client

    def get_events(self, **filters) -> list:
        """Get events from server with optional filters."""
        response = self.client.get("/events", params=filters)
        response.raise_for_status()
        return response.json()

    def get_events_for_run(self, run_id: str) -> list:
        """Get all events for a specific run."""
        response = self.client.get(f"/events/{run_id}")
        response.raise_for_status()
        return response.json()

    def get_run_sequence(self, run_id: str) -> dict:
        """Get event sequence for a run."""
        response = self.client.get(f"/events/{run_id}/sequence")
        response.raise_for_status()
        return response.json()

    def get_stats(self) -> dict:
        """Get server statistics."""
        response = self.client.get("/stats")
        response.raise_for_status()
        return response.json()

    def get_validation_summary(self) -> dict:
        """Get validation summary."""
        response = self.client.get("/validation/summary")
        response.raise_for_status()
        return response.json()

    def simulate_error_sequence(
        self, response_sequence: list[int], enabled: bool = True, delay_ms: int = 0
    ) -> dict:
        """Configure deterministic error sequence simulation."""
        payload = {"enabled": enabled, "delay_ms": delay_ms, "response_sequence": response_sequence}
        response = self.client.post("/simulate/sequence", json=payload)
        response.raise_for_status()
        return response.json()

    def reset(self) -> dict:
        """Reset server state."""
        response = self.client.post("/reset")
        response.raise_for_status()
        return response.json()

    def wait_for_events(self, expected_count: int, timeout: float = 10.0) -> bool:
        """Wait for a specific number of events to be received."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            stats = self.get_stats()
            if stats["total_events"] >= expected_count:
                return True
            time.sleep(0.1)
        return False

    def wait_for_run_events(self, run_id: str, expected_count: int, timeout: float = 10.0) -> bool:
        """Wait for a specific number of events for a run."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            events = self.get_events_for_run(run_id)
            if len(events) >= expected_count:
                return True
            time.sleep(0.1)
        return False


@pytest.fixture
def server_helper(test_server_client: httpx.Client) -> TestServerHelper:
    """Helper for interacting with test server."""
    return TestServerHelper(test_server_client)


# Mock fixtures for HTTP transport testing


@pytest.fixture
def mock_http_session():
    """Fixture providing a mock HTTP session for testing."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_client.post.return_value = mock_response

    # Return both the client and response for flexibility
    return mock_client, mock_response


@pytest.fixture
def mock_http_session_class(mock_http_session):
    """Fixture providing a mock Session class that returns the mock session."""
    mock_client, mock_response = mock_http_session

    with patch("openlineage.client.transport.http.Session") as mock_session_class:
        # Configure the context manager
        mock_session_class.return_value = mock_client
        yield mock_session_class, mock_client, mock_response


@pytest.fixture
def mock_async_http_client_class(mock_http_session):
    """Fixture providing a mock AsyncClient class that returns the mock client."""
    mock_client, mock_response = mock_http_session

    with patch("httpx.AsyncClient") as mock_client_class:
        # Configure the async context manager
        mock_client_class.return_value = mock_client
        yield mock_client_class, mock_client, mock_response
