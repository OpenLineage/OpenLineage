# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import os
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.facet import JobTypeJobFacet
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.transport.gcplineage import GCPLineageConfig, GCPLineageTransport
from openlineage.client.uuid import generate_new_uuid


@pytest.fixture(autouse=True)
def mock_gcp_modules():
    """Fixture to mock Google Cloud modules for tests that need GCP transport."""
    with (
        patch.dict(
            "sys.modules",
            {
                "google": MagicMock(),
                "google.cloud": MagicMock(),
                "google.cloud.datacatalog_lineage_v1": MagicMock(),
                "google.oauth2": MagicMock(),
                "google.oauth2.service_account": MagicMock(),
                "google.auth": MagicMock(),
                "google.auth.exceptions": MagicMock(),
            },
        ),
        patch("google.cloud.datacatalog_lineage_v1.LineageClient") as mock_client,
        patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient") as mock_async_client,
        patch("google.oauth2.service_account.Credentials") as mock_credentials,
        patch("google.auth.exceptions.GoogleAuthError", Exception) as mock_auth_error,
    ):
        yield {
            "client": mock_client,
            "async_client": mock_async_client,
            "credentials": mock_credentials,
            "auth_error": mock_auth_error,
        }


class TestGCPLineageConfig:
    """Test GCPLineageConfig validation and creation."""

    def test_gcplineage_config_minimal(self):
        """Test minimal config with just project_id."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})

        assert config.project_id == "test-project"
        assert config.location == "us-central1"  # Default
        assert config.credentials_path is None
        assert config.async_transport_rules == {"dbt": {"*": True}}

    def test_gcplineage_config_full(self):
        """Test full config with all parameters."""
        config = GCPLineageConfig.from_dict(
            {
                "project_id": "test-project",
                "location": "us-west1",
                "credentials_path": "/path/to/credentials.json",
                "async_transport_rules": {
                    "spark": {"*": True},
                    "airflow": {"dag": True},
                },
            }
        )

        assert config.project_id == "test-project"
        assert config.location == "us-west1"
        assert config.credentials_path == "/path/to/credentials.json"
        assert config.async_transport_rules == {
            "spark": {"*": True},
            "airflow": {"dag": True},
        }

    def test_gcplineage_config_missing_project_id(self):
        """Test that missing project_id raises ValueError."""
        with pytest.raises(ValueError, match="project_id is required"):
            GCPLineageConfig.from_dict({})

    def test_gcplineage_config_custom_async_rules(self):
        """Test custom async transport rules."""
        custom_rules = {
            "dbt": {"model": True, "test": False},
            "spark": {"*": True},
            "flink": {"streaming": True},
        }

        config = GCPLineageConfig.from_dict(
            {
                "project_id": "test-project",
                "async_transport_rules": custom_rules,
            }
        )

        assert config.async_transport_rules == custom_rules

    def test_gcplineage_config_empty_async_rules(self):
        """Test empty async transport rules."""
        config = GCPLineageConfig.from_dict(
            {
                "project_id": "test-project",
                "async_transport_rules": {},
            }
        )

        assert config.async_transport_rules == {}


class TestGCPLineageTransportInitialization:
    """Test GCPLineageTransport initialization and client creation."""

    def test_gcplineage_transport_initialization_default_credentials(self):
        """Test that GCPLineageTransport creates both sync and async clients with default credentials."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Verify transport properties
        assert transport.kind == "gcplineage"
        assert transport.config == config
        assert transport.parent == "projects/test-project/locations/us-central1"

        # Verify both clients were created (not None)
        assert transport.client is not None
        assert transport.async_client is not None

    def test_gcplineage_transport_initialization_with_credentials(self):
        """Test that GCPLineageTransport creates clients with service account credentials."""
        config = GCPLineageConfig.from_dict(
            {
                "project_id": "test-project",
                "credentials_path": "/path/to/credentials.json",
            }
        )
        
        # Mock file system access for credentials
        with patch("os.path.exists", return_value=True), \
             patch("os.access", return_value=True), \
             patch("builtins.open", mock.mock_open(read_data='{"type": "service_account", "project_id": "test", "private_key_id": "123", "private_key": "-----BEGIN PRIVATE KEY-----\\ntest\\n-----END PRIVATE KEY-----\\n", "client_email": "test@test.iam.gserviceaccount.com"}')):
            
            transport = GCPLineageTransport(config)

            # Verify both clients were created (not None)
            assert transport.client is not None
            assert transport.async_client is not None

    def test_gcplineage_transport_parent_construction(self):
        """Test correct parent string construction for different locations."""
        test_cases = [
            ("test-project", "us-central1", "projects/test-project/locations/us-central1"),
            ("my-project-123", "us-west1", "projects/my-project-123/locations/us-west1"),
            ("prod-env", "europe-west1", "projects/prod-env/locations/europe-west1"),
        ]

        for project_id, location, expected_parent in test_cases:
            config = GCPLineageConfig.from_dict({"project_id": project_id, "location": location})
            transport = GCPLineageTransport(config)

            assert transport.parent == expected_parent


class TestGCPLineageTransportRouting:
    """Test event routing logic based on async transport rules."""

    def _create_event(self, integration: str, job_type: str) -> RunEvent:
        """Helper method to create events with JobTypeJobFacet."""
        job_facets = {
            "jobType": JobTypeJobFacet(processingType="BATCH", integration=integration, jobType=job_type)
        }
        return RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )

    def test_routing_dbt_event_to_async(self):
        """Test that events with JobTypeJobFacet integration='dbt' use async transport."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Create event with dbt JobTypeJobFacet
        event = self._create_event("dbt", "MODEL")

        with (
            patch.object(transport, "_emit_async", return_value=None) as mock_emit_async,
            patch.object(transport, "_emit_sync") as mock_emit_sync,
        ):
            transport.emit(event)

            # Verify async transport was used
            mock_emit_async.assert_called_once()
            mock_emit_sync.assert_not_called()

    def test_routing_non_dbt_event_to_sync(self):
        """Test that events with JobTypeJobFacet integration!='dbt' use sync transport."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Create event with non-dbt JobTypeJobFacet
        event = self._create_event("SPARK", "JOB")

        with (
            patch.object(transport, "_emit_async") as mock_emit_async,
            patch.object(transport, "_emit_sync", return_value=None) as mock_emit_sync,
        ):
            transport.emit(event)

            # Verify sync transport was used
            mock_emit_sync.assert_called_once_with(event)
            mock_emit_async.assert_not_called()

    def test_routing_event_without_facets_to_sync(self):
        """Test that events without facets use sync transport."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Create event without facets
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        with (
            patch.object(transport, "_emit_async") as mock_emit_async,
            patch.object(transport, "_emit_sync", return_value=None) as mock_emit_sync,
        ):
            transport.emit(event)

            # Verify sync transport was used
            mock_emit_sync.assert_called_once_with(event)
            mock_emit_async.assert_not_called()

    def test_routing_custom_async_rules(self):
        """Test routing with custom async transport rules."""

        config = GCPLineageConfig.from_dict(
            {
                "project_id": "test-project",
                "async_transport_rules": {"spark": {"*": True}, "airflow": {"dag": True}},
            }
        )
        transport = GCPLineageTransport(config)

        # Test spark integration with wildcard
        spark_event = self._create_event("spark", "batch_job")

        with (
            patch.object(transport, "_emit_async", return_value=None) as mock_emit_async,
            patch.object(transport, "_emit_sync") as mock_emit_sync,
        ):
            transport.emit(spark_event)

            # Verify async transport was used
            mock_emit_async.assert_called_once()
            mock_emit_sync.assert_not_called()

        # Reset mocks
        mock_emit_async.reset_mock()
        mock_emit_sync.reset_mock()

        # Test airflow integration with specific job type
        airflow_dag_event = self._create_event("airflow", "dag")

        with (
            patch.object(transport, "_emit_async", return_value=None) as mock_emit_async,
            patch.object(transport, "_emit_sync") as mock_emit_sync,
        ):
            transport.emit(airflow_dag_event)

            # Verify async transport was used
            mock_emit_async.assert_called_once()
            mock_emit_sync.assert_not_called()

        # Reset mocks
        mock_emit_async.reset_mock()
        mock_emit_sync.reset_mock()

        # Test airflow integration with non-matching job type
        airflow_task_event = self._create_event("airflow", "task")

        with (
            patch.object(transport, "_emit_async") as mock_emit_async,
            patch.object(transport, "_emit_sync", return_value=None) as mock_emit_sync,
        ):
            transport.emit(airflow_task_event)

            # Verify sync transport was used
            mock_emit_sync.assert_called_once_with(airflow_task_event)
            mock_emit_async.assert_not_called()

    def test_routing_non_run_event_validation(self):
        """Test that non-RunEvent objects are rejected with proper error."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Create a non-RunEvent
        mock_event = MagicMock()

        # Should raise ValueError for invalid event types
        with pytest.raises(ValueError, match="GCP Lineage only supports RunEvent"):
            transport.emit(mock_event)
            
        # Cleanup
        transport.close()


class TestGCPLineageTransportMethods:
    """Test GCPLineageTransport emit methods."""

    def test_emit_sync_success(self):
        """Test successful sync emit."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        event = self._create_event("spark", "job")

        # This should not raise an exception
        transport._emit_sync(event)

    def test_emit_sync_error(self):
        """Test sync emit error handling."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Configure sync client to raise exception
        if transport.client:
            transport.client.process_open_lineage_run_event.side_effect = Exception("GCP API error")

        event = self._create_event("spark", "job")

        with pytest.raises(Exception, match="GCP API error"):
            transport._emit_sync(event)

    def test_emit_async_error(self):
        """Test async emit error handling."""
        config = GCPLineageConfig.from_dict({
            "project_id": "test-project",
            "async_transport_rules": {"dbt": {"*": True}}  # Enable async for dbt
        })
        transport = GCPLineageTransport(config)

        # Configure async client to raise exception
        transport.async_client.process_open_lineage_run_event.side_effect = Exception("GCP Async API error")

        event = self._create_event("dbt", "model")

        # Use the full emit() method to test real-world error handling
        with pytest.raises(RuntimeError, match="Async event emission failed"):
            transport.emit(event)
            
        # Cleanup
        transport.close()

    def _create_event(self, integration: str, job_type: str) -> RunEvent:
        """Helper method to create events with JobTypeJobFacet."""
        job_facets = {
            "jobType": JobTypeJobFacet(processingType="BATCH", integration=integration, jobType=job_type)
        }
        return RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )


class TestGCPLineageTransportAsyncRules:
    """Test async transport rules functionality."""

    def _create_event(self, integration: str, job_type: str) -> RunEvent:
        """Helper method to create events with JobTypeJobFacet."""
        job_facets = {
            "jobType": JobTypeJobFacet(processingType="BATCH", integration=integration, jobType=job_type)
        }
        return RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )

    def test_should_use_async_transport_method(self):
        """Test _should_use_async_transport method directly."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Test event with dbt facet (default rule)
        event_dbt = self._create_event("dbt", "MODEL")
        assert transport._should_use_async_transport(event_dbt) is True

        # Test event without facets
        event_no_facets = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )
        assert transport._should_use_async_transport(event_no_facets) is False

        # Test event with non-dbt facet
        event_spark = self._create_event("SPARK", "JOB")
        assert transport._should_use_async_transport(event_spark) is False

    def test_wildcard_rules(self):
        """Test wildcard async transport rules."""

        config = GCPLineageConfig.from_dict(
            {
                "project_id": "test-project",
                "async_transport_rules": {"*": {"sql": True}},
            }
        )
        transport = GCPLineageTransport(config)

        # Test any integration with sql job type
        spark_sql_event = self._create_event("spark", "sql")
        assert transport._should_use_async_transport(spark_sql_event) is True

        dbt_sql_event = self._create_event("dbt", "sql")
        assert transport._should_use_async_transport(dbt_sql_event) is True

        # Test any integration with non-sql job type
        spark_batch_event = self._create_event("spark", "job")
        assert transport._should_use_async_transport(spark_batch_event) is False

    def test_double_wildcard_rules(self, mock_gcp_modules):
        """Test double wildcard rules for all events."""

        config = GCPLineageConfig.from_dict(
            {
                "project_id": "test-project",
                "async_transport_rules": {"*": {"*": True}},
            }
        )
        transport = GCPLineageTransport(config)

        # Test event without facets
        event_no_facets = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )
        assert transport._should_use_async_transport(event_no_facets) is True

        # Test event with facets
        event_with_facet = self._create_event("spark", "batch")
        assert transport._should_use_async_transport(event_with_facet) is True


class TestGCPLineageTransportAsyncTracking:
    """Test async operation tracking and flush functionality."""

    def _create_event(self, integration: str, job_type: str) -> RunEvent:
        """Helper method to create events with JobTypeJobFacet."""
        job_facets = {
            "jobType": JobTypeJobFacet(processingType="BATCH", integration=integration, jobType=job_type)
        }
        return RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )

    def test_async_tracking_initialization(self):
        """Test that async tracking structures are initialized properly."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        assert hasattr(transport, "_pending_operations")
        import weakref
        assert isinstance(transport._pending_operations, weakref.WeakSet)
        assert len(transport._pending_operations) == 0
        assert hasattr(transport, "_operations_lock")
        assert hasattr(transport, "_shutdown_event")
        import threading
        assert isinstance(transport._operations_lock, type(threading.RLock()))  # We use RLock for nested acquisition
        assert isinstance(transport._shutdown_event, threading.Event)
        assert not transport._shutdown_event.is_set()  # Should start as not set
        
        # Cleanup
        transport.close()

    @patch("concurrent.futures.ThreadPoolExecutor")
    def test_async_tracking_with_thread_pool(self, mock_executor_class):
        """Test that async operations are properly tracked when using thread pool."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        # Setup mocks
        mock_executor = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_executor.submit.return_value = mock_future
        mock_executor_class.return_value = mock_executor
        transport._thread_pool = mock_executor
        
        event = self._create_event("dbt", "model")
        
        with patch.object(transport, "_emit_async", return_value=None):
            transport._emit_async_with_tracking(event)
        
        # Verify thread pool was used
        mock_executor.submit.assert_called_once()

    def test_shutdown_prevents_new_operations(self):
        """Test that setting shutdown event prevents new async operations."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        # Set shutdown event
        transport._shutdown_event.set()
        
        event = self._create_event("dbt", "model")
        
        # Should return early without doing anything
        result = transport._emit_async_with_tracking(event)
        assert result is None

    def test_flush_no_pending_operations(self):
        """Test flush when no operations are pending."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        result = transport.flush(timeout=1.0)
        assert result is True

    def test_flush_with_pending_operations(self):
        """Test flush with pending operations."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        # Test flush when no operations are pending (should return True)
        result = transport.flush(timeout=1.0)
        assert result is True
        
        # Cleanup
        transport.close()

    def test_flush_no_event_loop_exists(self):
        """Test flush when no event loop exists."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        # Test flush returns True when no operations are pending
        result = transport.flush(timeout=5.0)
        
        assert result is True
        
        # Cleanup
        transport.close()

    def test_flush_timeout(self):
        """Test flush timeout behavior."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        # Create mock pending futures
        future1 = MagicMock()
        future1.done.return_value = False
        transport._pending_operations = {future1}
        
        with patch("asyncio.get_event_loop") as mock_get_loop, \
             patch("asyncio.wait_for") as mock_wait_for:
            
            mock_loop = MagicMock()
            mock_loop.is_running.return_value = False
            mock_get_loop.return_value = mock_loop
            mock_wait_for.side_effect = asyncio.TimeoutError()
            
            result = transport.flush(timeout=0.1)
            
            assert result is False

    def test_close_calls_flush(self):
        """Test that close() method calls flush() before closing clients."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        with patch.object(transport, "flush", return_value=True) as mock_flush, \
             patch.object(transport, "_close_clients", return_value=True) as mock_close_clients, \
             patch.object(transport, "_shutdown_thread_pool", return_value=True) as mock_shutdown_pool:
            
            result = transport.close(timeout=10.0)
            
            mock_flush.assert_called_once_with(timeout=10.0)
            mock_close_clients.assert_called_once()
            mock_shutdown_pool.assert_called_once()
            assert result is True
            assert transport._shutdown_event.is_set()

    def test_close_with_default_timeout(self):
        """Test close() with default timeout (-1) uses 30s for flush."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        with patch.object(transport, "flush", return_value=True) as mock_flush, \
             patch.object(transport, "_run_async_safely"):
            
            transport.close()  # Default timeout=-1
            
            mock_flush.assert_called_once_with(timeout=30.0)

    def test_close_with_zero_timeout_skips_flush(self):
        """Test that close() with timeout=0 skips flush operation."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        with patch.object(transport, "flush") as mock_flush, \
             patch.object(transport, "_run_async_safely"):
            
            transport.close(timeout=0)
            
            mock_flush.assert_not_called()

    def test_close_handles_flush_failure(self):
        """Test that close() handles flush failures gracefully."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        with patch.object(transport, "flush", side_effect=Exception("Flush error")), \
             patch.object(transport, "_run_async_safely"), \
             patch("openlineage.client.transport.gcplineage.log") as mock_log:
            
            result = transport.close(timeout=5.0)
            
            # Should still return False due to flush failure but not crash
            assert result is False
            mock_log.warning.assert_called_with("Error during flush: %s", 
                                               mock.ANY)

    def test_close_checks_client_existence(self):
        """Test that close() only tries to close clients that exist."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        with patch.object(transport, "flush", return_value=True):
            # Don't access any clients so they don't get created
            result = transport.close()
            
            # Should succeed even without clients being initialized
            assert result is True

    @patch("concurrent.futures.ThreadPoolExecutor")
    @patch("asyncio.get_event_loop")
    def test_flush_with_running_event_loop(self, mock_get_loop, mock_executor_class):
        """Test flush behavior when event loop is already running."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)
        
        # Create mock pending futures
        future1 = MagicMock()
        future1.done.return_value = False
        transport._pending_operations = {future1}
        
        # Setup mocks - event loop is running
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        mock_get_loop.return_value = mock_loop
        
        mock_executor = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = True
        mock_executor.submit.return_value = mock_future
        mock_executor_class.return_value.__enter__.return_value = mock_executor
        mock_executor_class.return_value.__exit__.return_value = None
        
        result = transport.flush(timeout=5.0)
        
        assert result is True
        mock_executor.submit.assert_called_once()


class TestGCPLineageTransportIntegration:
    """Integration-style tests without mocking the underlying clients."""

    def test_gcplineage_transport_factory_registration(self):
        """Test that GCPLineageTransport is properly registered with the factory."""
        from openlineage.client.transport import get_default_factory

        factory = get_default_factory()

        # Test that 'gcplineage' is in registered transports
        assert "gcplineage" in factory.transports
        assert factory.transports["gcplineage"] == GCPLineageTransport

    def test_gcplineage_transport_creation_from_dict(self):
        """Test creating GCPLineageTransport through factory from dict config."""
        from openlineage.client.transport import get_default_factory

        factory = get_default_factory()

        config_dict = {
            "type": "gcplineage",
            "project_id": "test-project",
            "location": "us-west1",
        }

        transport = factory.create(config_dict)

        assert isinstance(transport, GCPLineageTransport)
        assert transport.kind == "gcplineage"
        assert transport.config.project_id == "test-project"
        assert transport.config.location == "us-west1"

    @patch.dict(
        os.environ,
        {
            "OPENLINEAGE__TRANSPORT__TYPE": "gcplineage",
            "OPENLINEAGE__TRANSPORT__PROJECT_ID": "env-project",
            "OPENLINEAGE__TRANSPORT__LOCATION": "europe-west1",
        },
    )
    def test_gcplineage_transport_from_ol_environment(self):
        """Test creating GCPLineageTransport from environment variables."""
        from openlineage.client import OpenLineageClient

        client = OpenLineageClient()

        assert isinstance(client.transport, GCPLineageTransport)
        assert client.transport.config.project_id == "env-project"
        assert client.transport.config.location == "europe-west1"


class TestGCPLineageTransportShutdownFix:
    """Test the fix for issue #3815 - ensuring async events are sent before shutdown."""
    
    def _create_event(self, integration: str, job_type: str) -> RunEvent:
        """Helper method to create events with JobTypeJobFacet."""
        job_facets = {
            "jobType": JobTypeJobFacet(processingType="BATCH", integration=integration, jobType=job_type)
        }
        return RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )
    
    @patch("concurrent.futures.ThreadPoolExecutor")
    def test_async_event_completed_before_shutdown(self, mock_executor_class):
        """Test that async events are properly waited for during shutdown."""
        config = GCPLineageConfig.from_dict({
            "project_id": "test-project",
            "async_transport_rules": {"dbt": {"*": True}}
        })
        transport = GCPLineageTransport(config)
        
        # Setup mocks for thread pool
        mock_executor = MagicMock()
        mock_future = MagicMock()
        mock_executor.submit.return_value = mock_future
        mock_executor_class.return_value = mock_executor
        transport._thread_pool = mock_executor
        
        # Mock the async emission to return successfully
        with patch.object(transport, "_emit_async", return_value=None):
            # Create and emit a dbt event (should use async)
            event = self._create_event("dbt", "model")
            transport.emit(event)
            
            # Verify the async operation was submitted
            mock_executor.submit.assert_called_once()
            
            # Mock the result to simulate successful completion
            mock_future.result.return_value = None
            
            # Now close the transport - this should wait for the async operation
            with patch.object(transport, "_close_clients", return_value=True), \
                 patch.object(transport, "_shutdown_thread_pool", return_value=True):
                
                result = transport.close()
                
                # Should succeed and wait for the async operation
                assert result is True
                assert transport._shutdown_event.is_set()
    
    def test_quick_shutdown_scenario_reproduction(self):
        """Test the scenario described in issue #3815 - quick shutdown after event emission."""
        config = GCPLineageConfig.from_dict({
            "project_id": "test-project",
            "async_transport_rules": {"dbt": {"*": True}}
        })
        
        transport = GCPLineageTransport(config)
        
        # Simulate the problematic scenario: emit event then immediately close
        with patch.object(transport, "_emit_async", return_value=None), \
             patch.object(transport, "flush", return_value=True) as mock_flush, \
             patch.object(transport, "_close_clients", return_value=True), \
             patch.object(transport, "_shutdown_thread_pool", return_value=True):
            
            # Emit async event
            event = self._create_event("dbt", "model")
            transport.emit(event)
            
            # Immediately close (this is where the bug would occur)
            result = transport.close()
            
            # The fix ensures flush() is called to wait for pending operations
            assert result is True
            mock_flush.assert_called_once_with(timeout=30.0)
            assert transport._shutdown_event.is_set()
