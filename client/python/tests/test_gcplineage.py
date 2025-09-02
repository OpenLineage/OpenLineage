# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.facet import JobTypeJobFacet
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.transport.gcplineage import GCPLineageConfig, GCPLineageTransport
from openlineage.client.uuid import generate_new_uuid

# Mock the google cloud modules at module level since they may not be installed
sys.modules["google"] = MagicMock()
sys.modules["google.cloud"] = MagicMock()
sys.modules["google.cloud.datacatalog_lineage_v1"] = MagicMock()
sys.modules["google.oauth2"] = MagicMock()
sys.modules["google.oauth2.service_account"] = MagicMock()


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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_gcplineage_transport_initialization_default_credentials(
        self, mock_async_client, mock_sync_client
    ):
        """Test that GCPLineageTransport creates both sync and async clients with default credentials."""
        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Verify transport properties
        assert transport.kind == "gcplineage"
        assert transport.config == config
        assert transport.parent == "projects/test-project/locations/us-central1"

        # Verify both clients were created with default credentials
        mock_sync_client.assert_called_once_with()
        mock_async_client.assert_called_once_with()

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    @patch("google.oauth2.service_account")
    def test_gcplineage_transport_initialization_with_credentials(
        self, mock_service_account, mock_async_client, mock_sync_client
    ):
        """Test that GCPLineageTransport creates clients with service account credentials."""
        mock_credentials = MagicMock()
        mock_service_account.Credentials.from_service_account_file.return_value = mock_credentials

        config = GCPLineageConfig.from_dict(
            {
                "project_id": "test-project",
                "credentials_path": "/path/to/credentials.json",
            }
        )
        GCPLineageTransport(config)

        # Verify credentials were loaded
        mock_service_account.Credentials.from_service_account_file.assert_called_once_with(
            "/path/to/credentials.json"
        )

        # Verify both clients were created with credentials
        mock_sync_client.assert_called_once_with(credentials=mock_credentials)
        mock_async_client.assert_called_once_with(credentials=mock_credentials)

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_gcplineage_transport_parent_construction(self, mock_async_client, mock_sync_client):
        """Test correct parent string construction for different locations."""
        test_cases = [
            ("test-project", "us-central1", "projects/test-project/locations/us-central1"),
            ("my-project-123", "us-west1", "projects/my-project-123/locations/us-west1"),
            ("prod-env", "europe-west1", "projects/prod-env/locations/europe-west1"),
        ]

        for project_id, location, expected_parent in test_cases:
            mock_sync_client.reset_mock()
            mock_async_client.reset_mock()

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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_routing_dbt_event_to_async(self, mock_async_client_class, mock_sync_client_class):
        """Test that events with JobTypeJobFacet integration='dbt' use async transport."""
        # Create mock client instances
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_routing_non_dbt_event_to_sync(self, mock_async_client_class, mock_sync_client_class):
        """Test that events with JobTypeJobFacet integration!='dbt' use sync transport."""
        # Create mock client instances
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_routing_event_without_facets_to_sync(self, mock_async_client_class, mock_sync_client_class):
        """Test that events without facets use sync transport."""
        # Create mock client instances
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_routing_custom_async_rules(self, mock_async_client_class, mock_sync_client_class):
        """Test routing with custom async transport rules."""
        # Create mock client instances
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_routing_non_run_event_warning(self, mock_async_client_class, mock_sync_client_class):
        """Test that non-RunEvent objects are handled with warning."""
        # Create mock client instances
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        # Create a non-RunEvent
        mock_event = MagicMock()

        with patch("openlineage.client.transport.gcplineage.log") as mock_log:
            result = transport.emit(mock_event)

            mock_log.warning.assert_called_once_with("GCP Lineage only supports RunEvent")
            assert result is None


class TestGCPLineageTransportMethods:
    """Test GCPLineageTransport emit methods."""

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_emit_sync_success(self, mock_async_client_class, mock_sync_client_class):
        """Test successful sync emit."""
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        event = self._create_event("spark", "job")

        with (
            patch("json.loads") as mock_json_loads,
            patch("openlineage.client.transport.gcplineage.Serde") as mock_serde,
        ):
            mock_serde.to_json.return_value = '{"test": "data"}'
            mock_json_loads.return_value = {"test": "data"}

            transport._emit_sync(event)

            # Verify the sync client was called with correct parameters
            mock_sync_client.process_open_lineage_run_event.assert_called_once_with(
                parent="projects/test-project/locations/us-central1", open_lineage={"test": "data"}
            )

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_emit_sync_error(self, mock_async_client_class, mock_sync_client_class):
        """Test sync emit error handling."""
        # Create mock client instances
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

        # Configure sync client to raise exception
        mock_sync_client.process_open_lineage_run_event.side_effect = Exception("GCP API error")

        config = GCPLineageConfig.from_dict({"project_id": "test-project"})
        transport = GCPLineageTransport(config)

        event = self._create_event("spark", "job")

        with pytest.raises(Exception, match="GCP API error"):
            transport._emit_sync(event)

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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_should_use_async_transport_method(self, mock_async_client_class, mock_sync_client_class):
        """Test _should_use_async_transport method directly."""
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_wildcard_rules(self, mock_async_client_class, mock_sync_client_class):
        """Test wildcard async transport rules."""
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

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

    @patch("google.cloud.datacatalog_lineage_v1.LineageClient")
    @patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient")
    def test_double_wildcard_rules(self, mock_async_client_class, mock_sync_client_class):
        """Test double wildcard rules for all events."""
        mock_sync_client = MagicMock()
        mock_async_client = MagicMock()
        mock_sync_client_class.return_value = mock_sync_client
        mock_async_client_class.return_value = mock_async_client

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

        with (
            patch("google.cloud.datacatalog_lineage_v1.LineageClient"),
            patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient"),
        ):
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

        with (
            patch("google.cloud.datacatalog_lineage_v1.LineageClient"),
            patch("google.cloud.datacatalog_lineage_v1.LineageAsyncClient"),
        ):
            client = OpenLineageClient()

            assert isinstance(client.transport, GCPLineageTransport)
            assert client.transport.config.project_id == "env-project"
            assert client.transport.config.location == "europe-west1"
