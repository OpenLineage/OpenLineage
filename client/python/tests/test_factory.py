# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.transport import (
    DefaultTransportFactory,
    KafkaTransport,
    get_default_factory,
)
from openlineage.client.transport.http import HttpTransport
from openlineage.client.transport.noop import NoopTransport

from tests.transport import AccumulatingTransport, FakeTransport

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@patch.dict(
    os.environ,
    {"OPENLINEAGE_URL": "http://mock-url:5000", "OPENLINEAGE_ENDPOINT": "endpoint"},
)
def test_client_uses_default_http_factory() -> None:
    client = OpenLineageClient()
    assert isinstance(client.transport, HttpTransport)
    assert client.transport.url == "http://mock-url:5000"
    assert client.transport.endpoint == "endpoint"


def test_factory_registers_new_transports(mocker: MockerFixture, root: Path) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "openlineage.yml")})
    factory = DefaultTransportFactory()
    factory.register_transport("accumulating", clazz=AccumulatingTransport)
    assert isinstance(OpenLineageClient(factory=factory).transport, AccumulatingTransport)


def test_factory_registers_transports_from_string(mocker: MockerFixture, root: Path) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "openlineage.yml")})
    factory = DefaultTransportFactory()
    factory.register_transport(
        "accumulating",
        clazz="tests.transport.AccumulatingTransport",
    )
    assert isinstance(OpenLineageClient(factory=factory).transport, AccumulatingTransport)


def test_factory_registers_transports_from_yaml(mocker: MockerFixture, root: Path) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "openlineage.yml")})
    mocker.patch("os.getcwd", return_value=str(Path(__file__).parent))

    factory = DefaultTransportFactory()
    factory.register_transport(
        "accumulating",
        clazz="tests.transport.AccumulatingTransport",
    )
    assert isinstance(OpenLineageClient(factory=factory).transport, AccumulatingTransport)


def test_factory_registers_transports_from_yaml_config(mocker: MockerFixture, root: Path) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "config.yml")})
    factory = DefaultTransportFactory()
    factory.register_transport(
        "fake",
        clazz="tests.transport.FakeTransport",
    )
    assert isinstance(OpenLineageClient(factory=factory).transport, FakeTransport)


def test_factory_configures_http_transport_from_yaml_config(
    mocker: MockerFixture,
    root: Path,
) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "http.yml")})
    factory = DefaultTransportFactory()
    factory.register_transport("http", HttpTransport)
    transport = OpenLineageClient(factory=factory).transport
    assert isinstance(transport, HttpTransport)


def test_factory_registers_from_dict(mocker: MockerFixture, root: Path) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "openlineage.yml")})
    factory = DefaultTransportFactory()
    factory.register_transport(
        "fake",
        clazz="tests.transport.FakeTransport",
    )
    factory.register_transport(
        "accumulating",
        clazz="tests.transport.AccumulatingTransport",
    )
    config = {
        "type": "accumulating",
    }
    transport = factory.create(config=config)
    assert isinstance(transport, AccumulatingTransport)


def test_automatically_registers_http_kafka() -> None:
    factory = get_default_factory()
    assert HttpTransport in factory.transports.values()
    assert KafkaTransport in factory.transports.values()


def test_transport_decorator_registers(mocker: MockerFixture, root: Path) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "config.yml")})

    factory = DefaultTransportFactory()
    factory.register_transport("fake", FakeTransport)

    assert isinstance(OpenLineageClient(factory=factory).transport, FakeTransport)


@pytest.mark.parametrize(
    ("env_var_value", "should_be_noop"),
    [
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("false", False),
        ("False", False),
        ("FALSE", False),
    ],
)
def test_env_disables_client(env_var_value: str, should_be_noop: str) -> None:
    with patch.dict(os.environ, {"OPENLINEAGE_DISABLED": env_var_value}):
        transport = DefaultTransportFactory().create()
        is_noop = isinstance(transport, NoopTransport)
        assert is_noop is should_be_noop


def test_env_disabled_ignores_config(mocker: MockerFixture, root: Path) -> None:
    env = {
        "OPENLINEAGE_DISABLED": "true",
        "OPENLINEAGE_CONFIG": str(root / "config" / "config.yml"),
    }
    mocker.patch.dict(os.environ, env)
    factory = DefaultTransportFactory()
    factory.register_transport(
        "fake",
        clazz="tests.transport.FakeTransport",
    )
    assert isinstance(OpenLineageClient(factory=factory).transport, NoopTransport)
