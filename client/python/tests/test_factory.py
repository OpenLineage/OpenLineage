# Copyright 2018-2025 contributors to the OpenLineage project
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
    assert client.transport.priority == 0


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
    priority = 2
    config = {
        "type": "accumulating",
        "priority": f"{priority}",
    }
    transport = factory.create(config=config)
    assert isinstance(transport, AccumulatingTransport)
    assert transport.priority == priority


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


def test_wrong_priority_raises_error() -> None:
    factory = DefaultTransportFactory()
    with pytest.raises(
        ValueError, match="Error casting priority `non_int` to int for transport `doesnt_matter`"
    ):
        factory.create(
            {
                "type": "doesnt_matter",
                "priority": "non_int",
            }
        )


def test_priority_is_correctly_assigned() -> None:
    factory = DefaultTransportFactory()
    factory.register_transport(
        "fake",
        clazz="tests.transport.FakeTransport",
    )
    priority = 3
    transport = factory.create(
        {
            "type": "fake",
            "priority": f"{3}",
        }
    )
    assert transport.kind == "fake"
    assert transport.priority == priority


def test_factory_create_transport_missing_type() -> None:
    """Test error when transport type is missing from config."""
    factory = DefaultTransportFactory()
    config = {"url": "http://example.com"}  # Missing 'type' key

    with pytest.raises(KeyError):
        factory._create_transport(config)


def test_factory_create_transport_invalid_transport_class() -> None:
    """Test error when transport class is not a Transport subclass."""
    factory = DefaultTransportFactory()

    # Register a class that's not a Transport subclass
    class NotATransport:
        pass

    factory.register_transport("invalid", NotATransport)
    config = {"type": "invalid"}

    with pytest.raises(TypeError, match="Transport .* has to be class, and subclass of Transport"):
        factory._create_transport(config)


def test_factory_create_transport_invalid_config_class() -> None:
    """Test error when config class is not a Config subclass."""
    from openlineage.client.transport import Transport

    factory = DefaultTransportFactory()

    # Create a transport with invalid config_class
    class NotAConfig:
        pass

    class InvalidTransport(Transport):
        config_class = NotAConfig

        def emit(self, event):
            pass

    factory.register_transport("invalid_config", InvalidTransport)
    config = {"type": "invalid_config"}

    with pytest.raises(TypeError, match="Config .* has to be class, and subclass of Config"):
        factory._create_transport(config)


def test_factory_create_transport_with_name() -> None:
    """Test transport creation with custom name."""
    factory = DefaultTransportFactory()
    factory.register_transport("accumulating", AccumulatingTransport)

    config = {"type": "accumulating", "name": "my_transport"}
    transport = factory._create_transport(config)

    assert isinstance(transport, AccumulatingTransport)
    assert transport.name == "my_transport"
