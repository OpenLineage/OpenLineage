# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

from requests import Session

from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde
from openlineage.client.transport.http import HttpConfig, HttpTransport

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
        },
    )

    assert config.url == "http://backend:5000"
    assert config.endpoint == "api/v1/lineage"
    assert config.verify is False
    assert config.auth.api_key == "1500100900"
    assert config.session is None
    assert config.adapter is None


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
        run=Run(runId=str(uuid.uuid4())),
        job=Job(namespace="http", name="test"),
        producer="prod",
        schemaURL="schema",
    )

    client.emit(event)
    transport.session.post.assert_called_once_with(
        "http://backend:5000/api/v1/lineage",
        Serde.to_json(event),
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
        run=Run(runId=str(uuid.uuid4())),
        job=Job(namespace="http", name="test"),
        producer="prod",
        schemaURL="schema",
    )

    client.emit(event)
    transport.session.post.assert_called_once_with(
        "http://backend:5000/custom/lineage",
        Serde.to_json(event),
        timeout=5.0,
        verify=True,
    )


def test_http_config_configs_session() -> None:
    with Session() as s:
        config = HttpConfig(url="http://backend:5000/api/v1/lineage", session=s)
        assert config.url == "http://backend:5000/api/v1/lineage"
        assert config.verify is True
        assert not hasattr(config.auth, "api_key")
        assert config.session is s
        assert config.adapter is None
