# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import os
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import BaseEvent, Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde
from openlineage.client.transport.transform import EventTransformer, TransformConfig, TransformTransport
from openlineage.client.uuid import generate_new_uuid


class NoopEventTransformer(EventTransformer):
    def transform(self, event: BaseEvent) -> BaseEvent | None:
        return event


class AlwaysNoneEventTransformer(EventTransformer):
    def transform(self, event: BaseEvent) -> BaseEvent | None:  # noqa: ARG002
        return None


class SampleEventTransformer(EventTransformer):
    def transform(self, event: BaseEvent) -> BaseEvent | None:
        event.job.namespace = "new_value"
        return event


class AlwaysFailingTransformer(EventTransformer):
    def transform(self, event: BaseEvent) -> BaseEvent | None:  # noqa: ARG002
        msg = "Failed transformation !"
        raise ZeroDivisionError(msg)


class NotEventTransformerBasedTransformer:
    def transform(self, event: BaseEvent) -> BaseEvent | None:
        return event


def test_transform_config_from_dict_full_config() -> None:
    transport_dict = {
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
    }
    config = TransformConfig.from_dict(
        {
            "transport": transport_dict,
            "transformer_class": "some.class.to.be.Imported",
            "transformer_properties": {"first": "val", "second": "val2"},
        }
    )

    assert config.transport == transport_dict
    assert config.transformer_class == "some.class.to.be.Imported"
    assert config.transformer_properties == {"first": "val", "second": "val2"}


def test_transform_config_from_dict_minimal_config() -> None:
    config = TransformConfig.from_dict(
        {
            "transport": {"type": "console"},
            "transformer_class": "some.class.to.be.Imported",
        }
    )
    assert config.transport == {"type": "console"}
    assert config.transformer_class == "some.class.to.be.Imported"
    assert config.transformer_properties == {}


def test_transform_config_from_dict_missing_config() -> None:
    with pytest.raises(RuntimeError):
        TransformConfig.from_dict({"transport": {"type": "console"}})

    with pytest.raises(RuntimeError):
        TransformConfig.from_dict({"transformer_class": "some.class.to.be.Imported"})


def test_base_event_transformer_saves_config():
    config = {"first": "val", "second": "val2"}
    obj = EventTransformer(properties=config)
    assert obj.properties == config


def test_base_event_transformer_requires_transform_method():
    obj = EventTransformer(properties={})
    with pytest.raises(NotImplementedError):
        obj.transform(None)


def test_base_event_transformer_str():
    config = {"first": "val", "second": "val2"}
    obj = NoopEventTransformer(properties=config)
    obj_str = str(obj)
    assert obj_str == "<NoopEventTransformer({'first': 'val', 'second': 'val2'})>"


@patch("httpx.Client")
def test_client_with_transform_transport_emits(mock_client_class) -> None:
    # Mock the context manager and post method
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_client_class.return_value.__exit__.return_value = None

    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
            },
            "transformer_class": "tests.transform.test_transform.NoopEventTransformer",
        }
    )
    transport = TransformTransport(config)

    client = OpenLineageClient(transport=transport)
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )

    client.emit(event)

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args

    assert call_args.kwargs["url"] == "http://backend:5000/api/v1/lineage"
    assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

    # Verify the content is the serialized event
    actual_content = call_args.kwargs["content"]
    expected_content = Serde.to_json(event)
    assert actual_content == expected_content


@patch("httpx.Client")
def test_client_with_transform_transport_emits_modified_event(mock_client_class) -> None:
    # Mock the context manager and post method
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_client_class.return_value.__exit__.return_value = None

    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
            },
            "transformer_class": "tests.transform.test_transform.SampleEventTransformer",
        }
    )
    transport = TransformTransport(config)

    client = OpenLineageClient(transport=transport)
    now = datetime.datetime.now().isoformat()
    run_id = str(generate_new_uuid())
    event = RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(runId=run_id),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )
    modified_event = RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(runId=run_id),
        job=Job(namespace="new_value", name="test"),
        producer="prod",
    )

    client.emit(event)

    # Verify the post was called with the modified event
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args

    assert call_args.kwargs["url"] == "http://backend:5000/api/v1/lineage"
    assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

    # Verify the content is the serialized modified event
    actual_content = call_args.kwargs["content"]
    expected_content = Serde.to_json(modified_event)
    assert actual_content == expected_content

    # Assert the original event is unchanged
    assert event == RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(runId=run_id),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )


@patch("httpx.Client")
def test_client_with_transform_transport_skips_emission_when_transformed_event_is_none(
    mock_client_class,
) -> None:
    # Mock the context manager and post method
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_client_class.return_value.__exit__.return_value = None

    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
            },
            "transformer_class": "tests.transform.test_transform.AlwaysNoneEventTransformer",
        }
    )
    transport = TransformTransport(config)

    client = OpenLineageClient(transport=transport)
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )

    client.emit(event)

    # Should not be called since transformer returns None
    mock_client.post.assert_not_called()


@patch("httpx.Client")
def test_client_with_transform_transport_emits_modified_deprecated_event(mock_client_class) -> None:
    from openlineage.client.run import Job, Run, RunEvent, RunState

    # Mock the context manager and post method
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_client_class.return_value.__exit__.return_value = None

    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
            },
            "transformer_class": "tests.transform.test_transform.SampleEventTransformer",
        }
    )
    transport = TransformTransport(config)

    client = OpenLineageClient(transport=transport)
    now = datetime.datetime.now().isoformat()
    run_id = str(generate_new_uuid())
    event = RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(runId=run_id),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )
    modified_event = RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(runId=run_id),
        job=Job(namespace="new_value", name="test"),
        producer="prod",
    )

    client.emit(event)

    # Verify the post was called with the modified event
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args

    assert call_args.kwargs["url"] == "http://backend:5000/api/v1/lineage"
    assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

    # Verify the content is the serialized modified event
    actual_content = call_args.kwargs["content"]
    expected_content = Serde.to_json(modified_event)
    assert actual_content == expected_content

    # Assert the original event is unchanged
    assert event == RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(runId=run_id),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )


@patch("httpx.Client")
@patch.dict(
    os.environ,
    {
        "OPENLINEAGE__TRANSPORT__TYPE": "transform",
        "OPENLINEAGE__TRANSPORT__TRANSFORMER_CLASS": "tests.transform.test_transform.NoopEventTransformer",
        "OPENLINEAGE__TRANSPORT__TRANSPORT__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__TRANSPORT__URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORT__CUSTOM_HEADERS__CUSTOM_HEADER": "FIRST",
        "OPENLINEAGE__TRANSPORT__TRANSPORT__CUSTOM_HEADERS__ANOTHER_HEADER": "second",
    },
)
def test_transform_transport_from_env_vars_emits(mock_client_class):
    # Mock the context manager and post method
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_client_class.return_value.__exit__.return_value = None

    transport = OpenLineageClient().transport
    mock_event = MagicMock()

    with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
        transport.emit(mock_event)

    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args
    headers = call_args.kwargs["headers"]

    assert headers["custom_header"] == "FIRST"
    assert headers["another_header"] == "second"


def test_transform_transport_calls_transformer_and_inner_transport() -> None:
    config = TransformConfig.from_dict(
        {
            "transport": {"type": "console"},
            "transformer_class": "tests.transform.test_transform.AlwaysNoneEventTransformer",
        }
    )
    transport = TransformTransport(config)

    transport.transport = MagicMock()
    transformer = MagicMock()
    transformer.transform.return_value = {"transformed_event": "value"}
    transport.transformer = transformer

    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )
    transport.emit(event)

    transformer.transform.assert_called_once()
    transport.transport.emit.assert_called_once_with({"transformed_event": "value"})


def test_client_with_transform_transport_fails_initialization_if_transformer_is_not_proper_class() -> None:
    config = TransformConfig.from_dict(
        {
            "transport": {"type": "console"},
            "transformer_class": "tests.transform.test_transform.NotEventTransformerBasedTransformer",
        }
    )
    with pytest.raises(TypeError):
        TransformTransport(config)


@patch("httpx.Client")
def test_client_with_transform_transport_fails_when_transform_fails(mock_client_class) -> None:
    # Mock the context manager and post method
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_client.post.return_value = mock_response
    mock_client_class.return_value.__enter__.return_value = mock_client
    mock_client_class.return_value.__exit__.return_value = None

    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
            },
            "transformer_class": "tests.transform.test_transform.AlwaysFailingTransformer",
        }
    )
    transport = TransformTransport(config)

    client = OpenLineageClient(transport=transport)
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )

    with pytest.raises(ZeroDivisionError):
        client.emit(event)

    # Should not be called since transformer failed
    mock_client.post.assert_not_called()
