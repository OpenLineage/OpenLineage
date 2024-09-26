# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import gzip
import os
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpCompression, HttpConfig, HttpTransport
from openlineage.client.uuid import generate_new_uuid
from requests import Session

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_http_loads_full_config() -> None:
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
        },
    )

    assert config.url == "http://backend:5000"
    assert config.endpoint == "api/v1/lineage"
    assert config.verify is False
    assert config.auth.api_key == "1500100900"
    assert config.session is None
    assert config.adapter is None
    assert config.compression is HttpCompression.GZIP


def test_http_loads_minimal_config() -> None:
    config = HttpConfig.from_dict(
        {
            "type": "http",
            "url": "http://backend:5000/api/v1/lineage",
        },
    )
    assert config.url == "http://backend:5000/api/v1/lineage"
    assert config.verify is True
    assert not hasattr(config.auth, "api_key")
    assert config.session is None
    assert config.adapter is None
    assert config.compression is None


def test_client_with_http_transport_emits(mocker: MockerFixture) -> None:
    session = mocker.patch("requests.Session")
    config = HttpConfig.from_dict(
        {
            "type": "http",
            "url": "http://backend:5000",
            "session": session,
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
    transport.session.post.assert_called_once_with(
        url="http://backend:5000/api/v1/lineage",
        data=Serde.to_json(event),
        headers={},
        timeout=5.0,
        verify=True,
    )


def test_client_with_http_transport_emits_custom_endpoint(mocker: MockerFixture) -> None:
    config = HttpConfig.from_dict(
        {
            "type": "http",
            "url": "http://backend:5000",
            "endpoint": "custom/lineage",
            "session": mocker.patch("requests.Session"),
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
    transport.session.post.assert_called_once_with(
        url="http://backend:5000/custom/lineage",
        data=Serde.to_json(event),
        headers={},
        timeout=5.0,
        verify=True,
    )


def test_client_with_http_transport_emits_with_gzip_compression(mocker: MockerFixture) -> None:
    session = mocker.patch("requests.Session")
    config = HttpConfig.from_dict(
        {
            "type": "http",
            "url": "http://backend:5000",
            "session": session,
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
    session.post.assert_called_once()

    headers = session.post.call_args.kwargs["headers"]
    assert headers["Content-Encoding"] == "gzip"

    data = session.post.call_args.kwargs["data"]
    assert gzip.decompress(data) == (
        b'{"eventTime": "2024-04-12T18:04:58.134314", "eventType": "START", '
        b'"inputs": [], "job": {"facets": {}, "name": "test", "namespace": "http"}, "outputs": [], '
        b'"producer": "prod", "run": {"facets": {}, "runId": "75782cf3-8be4-49dc-83e5-d2cf6239c168"}, '
        b'"schemaURL": "schema"}'
    )


def test_http_config_configs_session() -> None:
    with Session() as s:
        config = HttpConfig(url="http://backend:5000/api/v1/lineage", session=s)
        assert config.url == "http://backend:5000/api/v1/lineage"
        assert config.verify is True
        assert not hasattr(config.auth, "api_key")
        assert config.session is s
        assert config.adapter is None


@patch("requests.Session.post")
def test_http_transport_custom_headers_applied(mock_post):
    custom_headers = {"X-Custom-Header": "CustomValue", "X-Another-Header": "AnotherValue"}

    config = HttpConfig(url="http://example.com", custom_headers=custom_headers)

    transport = HttpTransport(config)
    mock_event = MagicMock()

    with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
        transport.emit(mock_event)

    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    headers = kwargs["headers"]

    assert headers["X-Custom-Header"] == "CustomValue"
    assert headers["X-Another-Header"] == "AnotherValue"


@patch("requests.Session.post")
def test_http_transport_auth_and_custom_headers_applied(mock_post):
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

    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    headers = kwargs["headers"]

    assert headers["Authorization"] == auth_token
    assert headers["X-Custom-Header"] == "CustomValue"


@patch("requests.Session.post")
def test_http_transport_no_custom_headers(mock_post):
    config = HttpConfig(url="http://example.com")
    transport = HttpTransport(config)
    mock_event = MagicMock()

    with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
        transport.emit(mock_event)

    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    headers = kwargs["headers"]

    # Only the Content-Type header should be set if no custom headers
    assert headers["Content-Type"] == "application/json"
    assert "X-Custom-Header" not in headers


@patch("requests.Session.post")
def test_http_transport_compression_with_custom_headers(mock_post):
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

    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    headers = kwargs["headers"]
    data = kwargs["data"]

    assert headers["X-Custom-Header"] == "CustomValue"
    assert headers["Content-Encoding"] == "gzip"
    assert data == b"compressed_data"


@patch("requests.Session.post")
@patch.dict(
    os.environ,
    {
        "OPENLINEAGE__TRANSPORT__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__CUSTOM_HEADERS__CUSTOM_HEADER": "FIRST",
        "OPENLINEAGE__TRANSPORT__CUSTOM_HEADERS__ANOTHER_HEADER": "second",
    },
)
def test_http_transport_with_custom_headers_from_env_vars(mock_post):
    transport = OpenLineageClient().transport
    mock_event = MagicMock()

    with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'), patch(
        "gzip.compress", return_value=b"compressed_data"
    ):
        transport.emit(mock_event)

    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    headers = kwargs["headers"]

    assert headers["custom_header"] == "FIRST"
    assert headers["another_header"] == "second"
