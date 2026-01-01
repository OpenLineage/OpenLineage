# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.facet import JobTypeJobFacet
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.transport.datadog import DatadogConfig, DatadogTransport
from openlineage.client.uuid import generate_new_uuid


class TestDatadogConfig:
    """Test DatadogConfig validation and creation."""

    def test_datadog_config_minimal(self):
        """Test minimal config with just apiKey."""
        config = DatadogConfig.from_dict({"apiKey": "test-key"})

        assert config.apiKey == "test-key"
        assert config.site == "datadoghq.com"  # Default
        assert config.timeout == 5.0
        assert config.max_queue_size == 10000
        assert config.max_concurrent_requests == 100

    def test_datadog_config_full(self):
        """Test full config with all parameters."""
        config = DatadogConfig.from_dict(
            {
                "apiKey": "test-key",
                "site": "us3.datadoghq.com",
                "timeout": 10.0,
                "max_queue_size": 5000,
                "max_concurrent_requests": 50,
                "retry": {
                    "total": 3,
                    "backoff_factor": 0.5,
                    "status_forcelist": [500, 502],
                },
            }
        )

        assert config.apiKey == "test-key"
        assert config.site == "us3.datadoghq.com"
        assert config.timeout == 10.0
        assert config.max_queue_size == 5000
        assert config.max_concurrent_requests == 50
        assert config.retry["total"] == 3
        assert config.retry["backoff_factor"] == 0.5
        assert config.retry["status_forcelist"] == [500, 502]

    def test_datadog_config_all_sites(self):
        """Test all valid site configurations."""
        valid_sites = [
            "datadoghq.com",
            "us3.datadoghq.com",
            "us5.datadoghq.com",
            "datadoghq.eu",
            "ddog-gov.com",
            "ap1.datadoghq.com",
            "ap2.datadoghq.com",
            "datad0g.com",
        ]

        for site in valid_sites:
            config = DatadogConfig.from_dict({"apiKey": "test-key", "site": site})
            assert config.site == site

    def test_datadog_config_invalid_site(self):
        """Test that invalid sites raise ValueError."""
        with pytest.raises(ValueError, match="Invalid site 'invalid-site'"):
            DatadogConfig.from_dict({"apiKey": "test-key", "site": "invalid-site"})

    def test_datadog_config_missing_api_key(self):
        """Test that missing apiKey raises ValueError."""
        with pytest.raises(ValueError, match="apiKey is required"):
            DatadogConfig.from_dict({})

    @patch.dict(os.environ, {"DD_API_KEY": "env-api-key"})
    def test_datadog_config_api_key_from_env(self):
        """Test apiKey fallback to DD_API_KEY environment variable."""
        config = DatadogConfig.from_dict({})
        assert config.apiKey == "env-api-key"

    @patch.dict(os.environ, {"DD_SITE": "datadoghq.eu"})
    def test_datadog_config_site_from_env(self):
        """Test site fallback to DD_SITE environment variable."""
        config = DatadogConfig.from_dict({"apiKey": "test-key"})
        assert config.site == "datadoghq.eu"

    @patch.dict(os.environ, {"DD_API_KEY": "env-key", "DD_SITE": "us3.datadoghq.com"})
    def test_datadog_config_both_from_env(self):
        """Test both apiKey and site from environment variables."""
        config = DatadogConfig.from_dict({})
        assert config.apiKey == "env-key"
        assert config.site == "us3.datadoghq.com"

    def test_datadog_config_explicit_overrides_env(self):
        """Test that explicit config overrides environment variables."""
        with patch.dict(os.environ, {"DD_API_KEY": "env-key", "DD_SITE": "datadoghq.eu"}):
            config = DatadogConfig.from_dict({"apiKey": "explicit-key", "site": "us5.datadoghq.com"})
            assert config.apiKey == "explicit-key"
            assert config.site == "us5.datadoghq.com"

    @patch.dict(os.environ, {"DD_SITE": "invalid-site"})
    def test_datadog_config_invalid_site_from_env(self):
        """Test that invalid site from environment raises ValueError."""
        with pytest.raises(ValueError, match="Invalid site 'invalid-site'"):
            DatadogConfig.from_dict({"apiKey": "test-key"})

    def test_datadog_config_custom_url_as_site(self):
        """Test that custom URLs are accepted as site parameter."""
        valid_urls = [
            "https://custom-intake.example.com",
            "http://localhost:8080",
            "https://datadog-proxy.internal.company.com",
            "https://intake.datadog.staging.com/v1/traces",
        ]

        for custom_url in valid_urls:
            config = DatadogConfig.from_dict({"apiKey": "test-key", "site": custom_url})
            assert config.site == custom_url

    def test_datadog_config_invalid_url_as_site(self):
        """Test that invalid URLs are rejected as site parameter."""
        invalid_urls = [
            "not-a-url",
            "ftp://example.com",  # Wrong scheme
            "https://",  # Missing netloc
            "://example.com",  # Missing scheme
            "just-text",
        ]

        for invalid_url in invalid_urls:
            with pytest.raises(ValueError, match=f"Invalid site '{invalid_url}'"):
                DatadogConfig.from_dict({"apiKey": "test-key", "site": invalid_url})

    @patch.dict(os.environ, {"DD_SITE": "https://custom-datadog.example.com"})
    def test_datadog_config_custom_url_from_env(self):
        """Test custom URL from environment variable."""
        config = DatadogConfig.from_dict({"apiKey": "test-key"})
        assert config.site == "https://custom-datadog.example.com"

    @patch.dict(os.environ, {"DD_SITE": "invalid-url"})
    def test_datadog_config_invalid_url_from_env(self):
        """Test that invalid URL from environment raises ValueError."""
        with pytest.raises(ValueError, match="Invalid site 'invalid-url'"):
            DatadogConfig.from_dict({"apiKey": "test-key"})


class TestDatadogTransportInitialization:
    """Test DatadogTransport initialization and transport creation."""

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_datadog_transport_initialization(self, mock_async_http, mock_http):
        """Test that DatadogTransport creates both HTTP and AsyncHTTP transports."""
        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Verify transport properties
        assert transport.kind == "datadog"
        assert transport.config == config

        # Verify both transports were created
        mock_http.assert_called_once()
        mock_async_http.assert_called_once()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_datadog_transport_url_mapping(self, mock_async_http, mock_http):
        """Test correct URL mapping for different sites."""
        site_url_mapping = {
            "datadoghq.com": "https://data-obs-intake.datadoghq.com",
            "us3.datadoghq.com": "https://data-obs-intake.us3.datadoghq.com",
            "us5.datadoghq.com": "https://data-obs-intake.us5.datadoghq.com",
            "datadoghq.eu": "https://data-obs-intake.datadoghq.eu",
            "ddog-gov.com": "https://data-obs-intake.ddog-gov.com",
            "ap1.datadoghq.com": "https://data-obs-intake.ap1.datadoghq.com",
            "ap2.datadoghq.com": "https://data-obs-intake.ap2.datadoghq.com",
            "datad0g.com": "https://data-obs-intake.datad0g.com",
        }

        for site, expected_url in site_url_mapping.items():
            mock_http.reset_mock()
            mock_async_http.reset_mock()

            config = DatadogConfig.from_dict({"apiKey": "test-key", "site": site})
            transport = DatadogTransport(config)  # noqa: F841

            # Check HTTP transport config
            http_call_args = mock_http.call_args[0][0]
            assert http_call_args.url == expected_url
            assert http_call_args.compression.value == "gzip"
            assert http_call_args.auth.api_key == "test-key"

            # Check AsyncHTTP transport config
            async_call_args = mock_async_http.call_args[0][0]
            assert async_call_args.url == expected_url
            assert async_call_args.compression.value == "gzip"
            assert async_call_args.auth.api_key == "test-key"

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_datadog_transport_config_passing(self, mock_async_http, mock_http):
        """Test that configuration parameters are correctly passed to transports."""
        config = DatadogConfig.from_dict(
            {
                "apiKey": "test-key",
                "site": "datadoghq.com",
                "timeout": 15.0,
                "max_queue_size": 2000,
                "max_concurrent_requests": 25,
                "retry": {"total": 7, "backoff_factor": 0.8, "status_forcelist": [500, 502, 503]},
            }
        )
        transport = DatadogTransport(config)  # noqa: F841

        # Check HTTP transport config
        http_call_args = mock_http.call_args[0][0]
        assert http_call_args.timeout == 15.0
        assert http_call_args.retry["total"] == 7
        assert http_call_args.retry["backoff_factor"] == 0.8
        assert http_call_args.retry["status_forcelist"] == [500, 502, 503]

        # Check AsyncHTTP transport config
        async_call_args = mock_async_http.call_args[0][0]
        assert async_call_args.timeout == 15.0
        assert async_call_args.max_queue_size == 2000
        assert async_call_args.max_concurrent_requests == 25
        assert async_call_args.retry["total"] == 7
        assert async_call_args.retry["backoff_factor"] == 0.8
        assert async_call_args.retry["status_forcelist"] == [500, 502, 503]

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_datadog_transport_custom_url_handling(self, mock_async_http, mock_http):
        """Test that custom URLs are used directly as intake URLs."""
        custom_urls = [
            "https://custom-intake.example.com",
            "http://localhost:8080",
            "https://datadog-proxy.internal.company.com/v1/traces",
        ]

        for custom_url in custom_urls:
            mock_http.reset_mock()
            mock_async_http.reset_mock()

            config = DatadogConfig.from_dict({"apiKey": "test-key", "site": custom_url})
            transport = DatadogTransport(config)  # noqa: F841

            # Check HTTP transport config uses custom URL
            http_call_args = mock_http.call_args[0][0]
            assert http_call_args.url == custom_url
            assert http_call_args.compression.value == "gzip"
            assert http_call_args.auth.api_key == "test-key"

            # Check AsyncHTTP transport config uses custom URL
            async_call_args = mock_async_http.call_args[0][0]
            assert async_call_args.url == custom_url
            assert async_call_args.compression.value == "gzip"
            assert async_call_args.auth.api_key == "test-key"


class TestDatadogTransportRouting:
    """Test event routing logic based on JobTypeJobFacet."""

    def setUp(self):
        """Set up common test data."""
        self.config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_dbt_event_to_async(self, mock_async_http, mock_http):
        """Test that events with JobTypeJobFacet integration='dbt' use async transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create event with dbt JobTypeJobFacet
        job_facets = {"jobType": JobTypeJobFacet(processingType="BATCH", integration="dbt", jobType="MODEL")}
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        # Verify async transport was used
        mock_async_instance.emit.assert_called_once_with(event)
        mock_http_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_non_dbt_event_to_http(self, mock_async_http, mock_http):
        """Test that events with JobTypeJobFacet integration!='dbt' use HTTP transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create event with non-dbt JobTypeJobFacet
        job_facets = {"jobType": JobTypeJobFacet(processingType="BATCH", integration="SPARK", jobType="JOB")}
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        # Verify HTTP transport was used
        mock_http_instance.emit.assert_called_once_with(event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_event_without_facets_to_http(self, mock_async_http, mock_http):
        """Test that events without facets use HTTP transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create event without facets
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        # Verify HTTP transport was used
        mock_http_instance.emit.assert_called_once_with(event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_event_without_job_type_facet_to_http(self, mock_async_http, mock_http):
        """Test that events without JobTypeJobFacet use HTTP transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create event with other facets but no JobTypeJobFacet
        job_facets = {"someOtherFacet": {"key": "value"}}
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        # Verify HTTP transport was used
        mock_http_instance.emit.assert_called_once_with(event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_dbt_case_insensitive(self, mock_async_http, mock_http):
        """Test that dbt integration is case insensitive."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Test various case variations of 'dbt'
        dbt_variations = ["dbt", "DBT", "Dbt", "dBt"]

        for dbt_case in dbt_variations:
            mock_http_instance.reset_mock()
            mock_async_instance.reset_mock()

            job_facets = {
                "jobType": JobTypeJobFacet(processingType="BATCH", integration=dbt_case, jobType="MODEL")
            }
            event = RunEvent(
                eventType=RunState.START,
                eventTime="2024-01-01T00:00:00Z",
                run=Run(runId=str(generate_new_uuid())),
                job=Job(namespace="test", name="job", facets=job_facets),
                producer="test",
                schemaURL="test",
            )

            transport.emit(event)

            # Verify async transport was used for all cases
            mock_async_instance.emit.assert_called_once_with(event)
            mock_http_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_non_run_event_to_http(self, mock_async_http, mock_http):
        """Test that non-RunEvent objects use HTTP transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create a non-RunEvent
        mock_event = MagicMock()
        # Ensure it doesn't have job attribute
        del mock_event.job

        transport.emit(mock_event)

        # Verify HTTP transport was used
        mock_http_instance.emit.assert_called_once_with(mock_event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_event_with_none_facets_to_http(self, mock_async_http, mock_http):
        """Test that events with job.facets=None use HTTP transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create event with job.facets explicitly set to None
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=None),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        # Verify HTTP transport was used
        mock_http_instance.emit.assert_called_once_with(event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_event_missing_job_attribute(self, mock_async_http, mock_http):
        """Test that events without job attribute use HTTP transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create mock event without job attribute
        mock_event = MagicMock(spec=[])  # spec=[] means no attributes

        transport.emit(mock_event)

        # Verify HTTP transport was used
        mock_http_instance.emit.assert_called_once_with(mock_event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_event_job_without_facets_attribute(self, mock_async_http, mock_http):
        """Test that events where job has no facets attribute use HTTP transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create mock event with job but job has no facets attribute
        mock_event = MagicMock()
        mock_event.job = MagicMock(spec=[])  # job without facets attribute

        transport.emit(mock_event)

        # Verify HTTP transport was used
        mock_http_instance.emit.assert_called_once_with(mock_event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_job_type_facet_missing_integration(self, mock_async_http, mock_http):
        """Test routing when JobTypeJobFacet exists but has no integration attribute."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create JobTypeJobFacet without integration attribute
        mock_job_type_facet = MagicMock(spec=[])  # No integration attribute
        job_facets = {"jobType": mock_job_type_facet}
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        # Verify HTTP transport was used (getattr should return empty string)
        mock_http_instance.emit.assert_called_once_with(event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_routing_event_with_empty_facets_to_http(self, mock_async_http, mock_http):
        """Test that events with empty facets dictionary use HTTP transport."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        # Create event with empty facets dictionary
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets={}),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        # Verify HTTP transport was used
        mock_http_instance.emit.assert_called_once_with(event)
        mock_async_instance.emit.assert_not_called()


class TestDatadogTransportMethods:
    """Test DatadogTransport utility methods."""

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_close_method(self, mock_async_http, mock_http):
        """Test that close method calls close on both transports."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        # Configure return values
        mock_http_instance.close.return_value = True
        mock_async_instance.close.return_value = True

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        result = transport.close(timeout=30.0)

        # Verify both transports were closed with the timeout
        mock_http_instance.close.assert_called_once_with(30.0)
        mock_async_instance.close.assert_called_once_with(30.0)
        assert result is True

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_close_method_partial_failure(self, mock_async_http, mock_http):
        """Test close method when one transport fails to close properly."""
        # Create mock transports
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        # Configure return values - one fails
        mock_http_instance.close.return_value = True
        mock_async_instance.close.return_value = False

        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})
        transport = DatadogTransport(config)

        result = transport.close()

        # Verify both transports were closed
        mock_http_instance.close.assert_called_once_with(-1)
        mock_async_instance.close.assert_called_once_with(-1)
        assert result is False  # Should return False if any transport fails

    def test_should_use_async_transport_method(self):
        """Test _should_use_async_transport method directly."""
        config = DatadogConfig.from_dict({"apiKey": "test-key", "site": "datadoghq.com"})

        with (
            patch("openlineage.client.transport.datadog.HttpTransport"),
            patch("openlineage.client.transport.datadog.AsyncHttpTransport"),
        ):
            transport = DatadogTransport(config)

        # Test event with dbt facet
        job_facets = {"jobType": JobTypeJobFacet(processingType="BATCH", integration="dbt", jobType="MODEL")}
        event_dbt = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets),
            producer="test",
            schemaURL="test",
        )

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
        job_facets_spark = {
            "jobType": JobTypeJobFacet(processingType="BATCH", integration="SPARK", jobType="JOB")
        }
        event_spark = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets=job_facets_spark),
            producer="test",
            schemaURL="test",
        )

        assert transport._should_use_async_transport(event_spark) is False


class TestDatadogTransportAsyncRules:
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

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_default_async_rules(self, mock_async_http, mock_http):
        """Test default async transport rules (dbt -> *)."""
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key"})
        transport = DatadogTransport(config)

        # Test dbt integration uses async
        dbt_event = self._create_event("dbt", "model")
        transport.emit(dbt_event)
        mock_async_instance.emit.assert_called_once_with(dbt_event)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        # Test non-dbt integration uses HTTP
        spark_event = self._create_event("spark", "job")
        transport.emit(spark_event)
        mock_http_instance.emit.assert_called_once_with(spark_event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_custom_integration_rules(self, mock_async_http, mock_http):
        """Test custom integration rules."""
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict(
            {"apiKey": "test-key", "async_transport_rules": {"spark": {"*": True}, "airflow": {"dag": True}}}
        )
        transport = DatadogTransport(config)

        # Test spark integration with wildcard
        spark_event = self._create_event("spark", "batch_job")
        transport.emit(spark_event)
        mock_async_instance.emit.assert_called_once_with(spark_event)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        # Test airflow integration with specific job types
        airflow_dag_event = self._create_event("airflow", "dag")
        transport.emit(airflow_dag_event)
        mock_async_instance.emit.assert_called_once_with(airflow_dag_event)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        airflow_task_event = self._create_event("airflow", "task")
        transport.emit(airflow_task_event)
        mock_http_instance.emit.assert_called_once_with(airflow_task_event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_wildcard_integration_rules(self, mock_async_http, mock_http):
        """Test wildcard integration rules."""
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict(
            {"apiKey": "test-key", "async_transport_rules": {"*": {"sql": True}}}
        )
        transport = DatadogTransport(config)

        # Test any integration with ml_training job type
        spark_ml_event = self._create_event("spark", "sql")
        transport.emit(spark_ml_event)
        mock_async_instance.emit.assert_called_once_with(spark_ml_event)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        dbt_ml_event = self._create_event("dbt", "sql")
        transport.emit(dbt_ml_event)
        mock_async_instance.emit.assert_called_once_with(dbt_ml_event)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        # Test any integration with batch_processing job type (should use HTTP)
        spark_batch_event = self._create_event("spark", "job")
        transport.emit(spark_batch_event)
        mock_http_instance.emit.assert_called_once_with(spark_batch_event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_case_insensitive_matching(self, mock_async_http, mock_http):
        """Test case insensitive matching for integration and job type."""
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict(
            {"apiKey": "test-key", "async_transport_rules": {"SPARK": {"BatchJob": True}}}
        )
        transport = DatadogTransport(config)

        # Test various case combinations
        test_cases = [
            ("spark", "batchjob"),
            ("SPARK", "BATCHJOB"),
            ("Spark", "BatchJob"),
            ("sPaRk", "bAtChJoB"),
        ]

        for integration, job_type in test_cases:
            mock_http_instance.reset_mock()
            mock_async_instance.reset_mock()

            event = self._create_event(integration, job_type)
            transport.emit(event)
            mock_async_instance.emit.assert_called_once_with(event)
            mock_http_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_no_matching_rules_defaults_to_http(self, mock_async_http, mock_http):
        """Test that events without matching rules use HTTP transport."""
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict(
            {"apiKey": "test-key", "async_transport_rules": {"spark": {"job": True}}}
        )
        transport = DatadogTransport(config)

        # Test integration not in rules
        flink_event = self._create_event("flink", "streaming")
        transport.emit(flink_event)
        mock_http_instance.emit.assert_called_once_with(flink_event)
        mock_async_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        # Test job type not in rules for matching integration
        spark_stream_event = self._create_event("spark", "sql")
        transport.emit(spark_stream_event)
        mock_http_instance.emit.assert_called_once_with(spark_stream_event)
        mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_empty_rules_defaults_to_http(self, mock_async_http, mock_http):
        """Test that empty rules default all events to HTTP."""
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "async_transport_rules": {}})
        transport = DatadogTransport(config)

        # Test various events all use HTTP
        events = [
            self._create_event("dbt", "model"),
            self._create_event("spark", "batch"),
            self._create_event("airflow", "task"),
        ]

        for event in events:
            mock_http_instance.reset_mock()
            mock_async_instance.reset_mock()

            transport.emit(event)
            mock_http_instance.emit.assert_called_once_with(event)
            mock_async_instance.emit.assert_not_called()

    @patch("openlineage.client.transport.datadog.HttpTransport")
    @patch("openlineage.client.transport.datadog.AsyncHttpTransport")
    def test_double_wildcard_uses_async_even_without_job_facets(self, mock_async_http, mock_http):
        """Test that double wildcard configuration uses async even for events without JobTypeJobFacet."""
        mock_http_instance = MagicMock()
        mock_async_instance = MagicMock()
        mock_http.return_value = mock_http_instance
        mock_async_http.return_value = mock_async_instance

        config = DatadogConfig.from_dict({"apiKey": "test-key", "async_transport_rules": {"*": {"*": True}}})
        transport = DatadogTransport(config)

        # Test event without facets
        event_no_facets = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event_no_facets)
        mock_async_instance.emit.assert_called_once_with(event_no_facets)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        # Test event with empty facets
        event_empty_facets = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets={}),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event_empty_facets)
        mock_async_instance.emit.assert_called_once_with(event_empty_facets)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        # Test event with facets but no JobTypeJobFacet
        event_other_facets = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job", facets={"someOtherFacet": {"key": "value"}}),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event_other_facets)
        mock_async_instance.emit.assert_called_once_with(event_other_facets)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()

        # Test event with JobTypeJobFacet (should still use async)
        event_with_job_facet = self._create_event("spark", "batch")
        transport.emit(event_with_job_facet)
        mock_async_instance.emit.assert_called_once_with(event_with_job_facet)
        mock_http_instance.emit.assert_not_called()

        mock_http_instance.reset_mock()
        mock_async_instance.reset_mock()


class TestDatadogTransportIntegration:
    """Integration-style tests without mocking the underlying transports."""

    def test_datadog_transport_factory_registration(self):
        """Test that DatadogTransport is properly registered with the factory."""
        from openlineage.client.transport import get_default_factory

        factory = get_default_factory()

        # Test that 'datadog' is in registered transports
        assert "datadog" in factory.transports
        assert factory.transports["datadog"] == DatadogTransport

    def test_datadog_transport_creation_from_dict(self):
        """Test creating DatadogTransport through factory from dict config."""
        from openlineage.client.transport import get_default_factory

        factory = get_default_factory()

        config_dict = {"type": "datadog", "apiKey": "test-key", "site": "datadoghq.com", "timeout": 10.0}

        transport = factory.create(config_dict)

        assert isinstance(transport, DatadogTransport)
        assert transport.kind == "datadog"
        assert transport.config.apiKey == "test-key"
        assert transport.config.site == "datadoghq.com"
        assert transport.config.timeout == 10.0

    @patch.dict(
        os.environ,
        {
            "OPENLINEAGE__TRANSPORT__TYPE": "datadog",
            "OPENLINEAGE__TRANSPORT__APIKEY": "env-key",
            "OPENLINEAGE__TRANSPORT__SITE": "datadoghq.eu",
        },
    )
    def test_datadog_transport_from_ol_environment(self):
        """Test creating DatadogTransport from environment variables."""
        from openlineage.client import OpenLineageClient

        client = OpenLineageClient()

        assert isinstance(client.transport, DatadogTransport)
        assert client.transport.config.apiKey == "env-key"
        assert client.transport.config.site == "datadoghq.eu"
