# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import gzip
import os
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde
from openlineage.client.transport.http import (
    ApiKeyTokenProvider,
    HttpCompression,
    HttpConfig,
    HttpTransport,
)
from openlineage.client.uuid import generate_new_uuid


class TestHttpConfig:
    def test_http_loads_full_config(self):
        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "endpoint": "api/v1/lineage",
                "verify": False,
                "auth": {
                    "type": "api_key",
                    "api_key": "1500100900",
                },
                "compression": "gzip",
                "retry": {
                    "total": 7,
                    "connect": 3,
                    "read": 2,
                    "status": 5,
                    "other": 1,
                    "allowed_methods": ["POST"],
                    "status_forcelist": [500, 502, 503, 504],
                    "backoff_factor": 0.5,
                    "raise_on_redirect": False,
                    "raise_on_status": False,
                },
            },
        )

        assert config.url == "http://backend:5000"
        assert config.endpoint == "api/v1/lineage"
        assert config.verify is False
        assert config.auth.api_key == "1500100900"
        assert config.compression is HttpCompression.GZIP
        assert config.retry == {
            "total": 7,
            "connect": 3,
            "read": 2,
            "status": 5,
            "other": 1,
            "allowed_methods": ["POST"],
            "status_forcelist": [500, 502, 503, 504],
            "backoff_factor": 0.5,
            "raise_on_redirect": False,
            "raise_on_status": False,
        }

    def test_http_loads_minimal_config(self):
        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000/api/v1/lineage",
            },
        )
        assert config.url == "http://backend:5000/api/v1/lineage"
        assert config.verify is True
        assert not hasattr(config.auth, "api_key")
        assert config.compression is None

    def test_http_client_loads_retry(self):
        total = 7
        connect = 3
        read = 2
        status = 5
        other = 1
        backoff = 0.5

        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "retry": {
                    "total": total,
                    "connect": connect,
                    "read": read,
                    "status": status,
                    "other": other,
                    "allowed_methods": ["POST"],
                    "status_forcelist": [500, 502, 503, 504, 590],
                    "backoff_factor": backoff,
                    "raise_on_redirect": False,
                    "raise_on_status": False,
                },
            },
        )

        client = OpenLineageClient(transport=HttpTransport(config))
        assert client.transport.kind == "http"
        assert client.transport.url == "http://backend:5000"

        # The new implementation stores retry config directly
        retry = client.transport.config.retry
        assert retry["total"] == total
        assert retry["read"] == read
        assert retry["connect"] == connect
        assert retry["status"] == status
        assert retry["other"] == other
        assert retry["allowed_methods"] == ["POST"]
        assert retry["status_forcelist"] == [500, 502, 503, 504, 590]
        assert retry["backoff_factor"] == backoff
        assert retry["raise_on_redirect"] is False
        assert retry["raise_on_status"] is False


class TestHttpTransportSync:
    """Test HttpTransport (sync HTTP functionality)"""

    def test_http_transport_initialization(self):
        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        assert transport.config == config
        assert transport.url == "http://example.com"
        assert transport.endpoint == "api/v1/lineage"
        assert transport.timeout == 5.0
        assert transport.verify is True

    def test_http_transport_emit_success(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args

        assert call_args.kwargs["url"] == "http://example.com/api/v1/lineage"
        assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

    def test_http_transport_with_gzip_compression(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig(url="http://example.com", compression=HttpCompression.GZIP)
        transport = HttpTransport(config)

        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        transport.emit(event)

        call_args = mock_client.post.call_args

        assert call_args.kwargs["headers"]["Content-Encoding"] == "gzip"
        # Content should be compressed
        content = call_args.kwargs["data"]
        assert isinstance(content, bytes)
        # Should be able to decompress
        decompressed = gzip.decompress(content)
        assert len(decompressed) > 0

    def test_http_transport_invalid_url(self):
        with pytest.raises(ValueError, match="Need valid url"):
            HttpTransport(HttpConfig(url="not-a-url"))

        with pytest.raises(ValueError, match="Need valid url"):
            HttpTransport(HttpConfig(url=""))

    def test_http_transport_emit_basic(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        mock_event = MagicMock()
        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()

    def test_http_transport_session_reused(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        mock_event = MagicMock()
        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_session_class.assert_called_once()
        mock_client.post.assert_called_once()
        mock_client.close.assert_not_called()

    def test_http_transport_close(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        mock_event = MagicMock()
        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        transport.close()
        mock_client.close.assert_called_once()


class TestHttpRequestPreparation:
    def test_prepare_request_basic(self):
        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        event_json = '{"test": "event"}'
        body, headers = transport._prepare_request(event_json)

        assert body == event_json
        assert headers["Content-Type"] == "application/json"

    def test_prepare_request_with_gzip(self):
        config = HttpConfig(url="http://example.com", compression=HttpCompression.GZIP)
        transport = HttpTransport(config)

        event_json = '{"test": "event"}'
        body, headers = transport._prepare_request(event_json)

        assert isinstance(body, bytes)
        assert headers["Content-Type"] == "application/json"
        assert headers["Content-Encoding"] == "gzip"

        # Verify compression worked
        decompressed = gzip.decompress(body).decode("utf-8")
        assert decompressed == event_json

    def test_prepare_request_with_custom_headers(self):
        custom_headers = {"X-Custom": "value", "X-Another": "test"}
        config = HttpConfig(url="http://example.com", custom_headers=custom_headers)
        transport = HttpTransport(config)

        event_json = '{"test": "event"}'
        body, headers = transport._prepare_request(event_json)

        assert headers["Content-Type"] == "application/json"
        assert headers["X-Custom"] == "value"
        assert headers["X-Another"] == "test"

    def test_prepare_request_with_auth(self):
        from openlineage.client.transport.http import ApiKeyTokenProvider

        auth = ApiKeyTokenProvider({"api_key": "test-key"})
        config = HttpConfig(url="http://example.com", auth=auth)
        transport = HttpTransport(config)

        event_json = '{"test": "event"}'
        body, headers = transport._prepare_request(event_json)

        assert headers["Content-Type"] == "application/json"
        assert headers["Authorization"] == "Bearer test-key"


@pytest.mark.unit
class TestHttpMock:
    """Tests that test the complete HTTP transport flow"""

    def test_client_with_http_transport_emits(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
            },
        )
        transport = HttpTransport(config)

        client = OpenLineageClient(transport=transport)
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.datetime.now().isoformat(),
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="http", name="test"),
            producer="prod",
            schemaURL="schema",
        )

        client.emit(event)

        # Verify the post was called with correct parameters
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args

        assert call_args.kwargs["url"] == "http://backend:5000/api/v1/lineage"
        assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

        # Verify the content is the serialized event
        actual_content = call_args.kwargs["data"]
        expected_content = Serde.to_json(event)
        assert actual_content == expected_content

    def test_client_with_http_transport_emits_custom_endpoint(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "endpoint": "custom/lineage",
            },
        )
        transport = HttpTransport(config)

        client = OpenLineageClient(transport=transport)
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.datetime.now().isoformat(),
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="http", name="test"),
            producer="prod",
            schemaURL="schema",
        )

        client.emit(event)

        # Verify the post was called with correct custom endpoint
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args.kwargs["url"] == "http://backend:5000/custom/lineage"

    def test_client_with_http_transport_emits_with_gzip_compression(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "compression": "gzip",
            },
        )
        transport = HttpTransport(config)

        client = OpenLineageClient(transport=transport)
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-04-12T18:04:58.134314",
            run=Run(runId="75782cf3-8be4-49dc-83e5-d2cf6239c168"),
            job=Job(namespace="http", name="test"),
            producer="prod",
            schemaURL="schema",
        )

        client.emit(event)

        # Verify compression was applied
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args

        headers = call_args.kwargs["headers"]
        assert headers["Content-Encoding"] == "gzip"

        content = call_args.kwargs["data"]
        assert isinstance(content, bytes)

        # Verify content can be decompressed and matches expected JSON
        decompressed = gzip.decompress(content)
        expected_json = (
            b'{"eventTime": "2024-04-12T18:04:58.134314", "eventType": "START", '
            b'"inputs": [], "job": {"facets": {}, "name": "test", "namespace": "http"}, "outputs": [], '
            b'"producer": "prod", "run": {"facets": {}, "runId": "75782cf3-8be4-49dc-83e5-d2cf6239c168"}, '
            b'"schemaURL": "schema"}'
        )
        assert decompressed == expected_json

    def test_http_transport_custom_headers_applied(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        custom_headers = {"X-Custom-Header": "CustomValue", "X-Another-Header": "AnotherValue"}
        config = HttpConfig(url="http://example.com", custom_headers=custom_headers)
        transport = HttpTransport(config)
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]

        assert headers["X-Custom-Header"] == "CustomValue"
        assert headers["X-Another-Header"] == "AnotherValue"

    def test_http_transport_auth_and_custom_headers_applied(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        custom_headers = {"X-Custom-Header": "CustomValue"}
        auth_token = "Bearer test_token"  # noqa: S105

        # Set up config with an ApiKeyTokenProvider
        config = HttpConfig(
            url="http://example.com",
            auth=ApiKeyTokenProvider({"api_key": "test_token"}),
            custom_headers=custom_headers,
        )

        transport = HttpTransport(config)
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]

        assert headers["Authorization"] == auth_token
        assert headers["X-Custom-Header"] == "CustomValue"

    def test_http_transport_no_custom_headers(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]

        # Only the Content-Type header should be set if no custom headers
        assert headers["Content-Type"] == "application/json"
        assert "X-Custom-Header" not in headers

    def test_http_transport_compression_with_custom_headers(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        custom_headers = {"X-Custom-Header": "CustomValue"}

        config = HttpConfig(
            url="http://example.com", compression=HttpCompression.GZIP, custom_headers=custom_headers
        )
        transport = HttpTransport(config)
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'), patch(
            "gzip.compress", return_value=b"compressed_data"
        ):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]
        content = call_args.kwargs["data"]

        assert headers["X-Custom-Header"] == "CustomValue"
        assert headers["Content-Encoding"] == "gzip"
        assert content == b"compressed_data"

    @patch.dict(
        os.environ,
        {
            "OPENLINEAGE__TRANSPORT__TYPE": "http",
            "OPENLINEAGE__TRANSPORT__URL": "http://example.com",
            "OPENLINEAGE__TRANSPORT__CUSTOM_HEADERS__CUSTOM_HEADER": "FIRST",
            "OPENLINEAGE__TRANSPORT__CUSTOM_HEADERS__ANOTHER_HEADER": "second",
        },
    )
    def test_http_transport_with_custom_headers_from_env_vars(self, mock_http_session_class):
        mock_session_class, mock_client, mock_response = mock_http_session_class

        transport = OpenLineageClient().transport
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]

        assert headers["custom_header"] == "FIRST"
        assert headers["another_header"] == "second"
