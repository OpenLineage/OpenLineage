# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import os
import warnings
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import attr
import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import BaseEvent, Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import external_query_run
from openlineage.client.serde import Serde
from openlineage.client.transport.transform import EventTransformer, TransformConfig, TransformTransport
from openlineage.client.uuid import generate_new_uuid

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


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
    assert config.transformer_properties is None


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


def test_client_with_transform_transport_emits(mocker: MockerFixture) -> None:
    session = mocker.patch("requests.Session")
    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
                "session": session,
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
    transport.transport.session.post.assert_called_once_with(
        url="http://backend:5000/api/v1/lineage",
        data=Serde.to_json(event),
        headers={"Content-Type": "application/json"},
        timeout=5.0,
        verify=True,
    )


def test_client_with_transform_transport_emits_modified_event(mocker: MockerFixture) -> None:
    session = mocker.patch("requests.Session")
    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
                "session": session,
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
    transport.transport.session.post.assert_called_once_with(
        url="http://backend:5000/api/v1/lineage",
        data=Serde.to_json(modified_event),
        headers={"Content-Type": "application/json"},
        timeout=5.0,
        verify=True,
    )

    # Assert the original event is unchanged
    assert event == RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(runId=run_id),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )


def test_client_with_transform_transport_emits_modified_event_with_older_facets(
    mocker: MockerFixture,
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from openlineage.client.facet import BaseFacet as OldBaseFacet

        @attr.define
        class SomeOldRunFacet(OldBaseFacet):
            version: str = attr.ib()

    session = mocker.patch("requests.Session")
    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
                "session": session,
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
        run=Run(
            runId=run_id,
            facets={
                "externalQuery": external_query_run.ExternalQueryRunFacet(
                    externalQueryId="queryid", source="source"
                ),
                "old_facet": SomeOldRunFacet(version="2"),
            },
        ),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )
    modified_event = RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(
            runId=run_id,
            facets={
                "externalQuery": external_query_run.ExternalQueryRunFacet(
                    externalQueryId="queryid", source="source"
                ),
                "old_facet": SomeOldRunFacet(version="2"),
            },
        ),
        job=Job(namespace="new_value", name="test"),
        producer="prod",
    )

    client.emit(event)
    transport.transport.session.post.assert_called_once_with(
        url="http://backend:5000/api/v1/lineage",
        data=Serde.to_json(modified_event),
        headers={"Content-Type": "application/json"},
        timeout=5.0,
        verify=True,
    )

    # Assert the original event is unchanged
    assert event == RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(
            runId=run_id,
            facets={
                "externalQuery": external_query_run.ExternalQueryRunFacet(
                    externalQueryId="queryid", source="source"
                ),
                "old_facet": SomeOldRunFacet(version="2"),
            },
        ),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )


def test_client_with_transform_transport_skips_emission_when_transformed_event_is_none(
    mocker: MockerFixture,
) -> None:
    session = mocker.patch("requests.Session")
    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
                "session": session,
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

    transport.transport.session.post.assert_not_called()


def test_client_with_transform_transport_emits_modified_deprecated_event(mocker: MockerFixture) -> None:
    from openlineage.client.run import Job, Run, RunEvent, RunState

    session = mocker.patch("requests.Session")
    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
                "session": session,
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
    transport.transport.session.post.assert_called_once_with(
        url="http://backend:5000/api/v1/lineage",
        data=Serde.to_json(modified_event),
        headers={"Content-Type": "application/json"},
        timeout=5.0,
        verify=True,
    )

    # Assert the original event is unchanged
    assert event == RunEvent(
        eventType=RunState.START,
        eventTime=now,
        run=Run(runId=run_id),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )


@patch("requests.Session.post")
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
def test_transform_transport_from_env_vars_emits(mock_post):
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


def test_client_with_transform_transport_fails_when_transform_fails(mocker: MockerFixture) -> None:
    session = mocker.patch("requests.Session")
    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
                "session": session,
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

    transport.transport.session.post.assert_not_called()


def test_client_with_transform_transport_close(mocker: MockerFixture) -> None:
    session = mocker.patch("requests.Session")
    config = TransformConfig.from_dict(
        {
            "transport": {
                "type": "http",
                "url": "http://backend:5000",
                "session": session,
            },
            "transformer_class": "tests.transform.test_transform.NoopEventTransformer",
        }
    )
    transport = TransformTransport(config)

    transport.close()
    session.close.assert_called_once()
