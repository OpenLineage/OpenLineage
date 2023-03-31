# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
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

current_path = os.path.join(os.getcwd(), "tests")


@patch.dict(os.environ, {"OPENLINEAGE_URL": "http://mock-url:5000"})
def test_client_uses_default_http_factory():
    client = OpenLineageClient()
    assert isinstance(client.transport, HttpTransport)
    assert client.transport.url == "http://mock-url:5000"


@patch('openlineage.client.transport.factory.yaml')
@patch('os.listdir')
@patch('os.path.join')
def test_factory_registers_new_transports(join, listdir, yaml):
    listdir.return_value = "openlineage.yml"
    yaml.safe_load.return_value = {"transport": {"type": "accumulating"}}

    factory = DefaultTransportFactory()
    factory.register_transport("accumulating", clazz=AccumulatingTransport)
    transport = factory.create()
    assert isinstance(transport, AccumulatingTransport)


@patch('openlineage.client.transport.factory.yaml')
@patch('os.listdir')
@patch('os.path.join')
def test_factory_registers_transports_from_string(join, listdir, yaml):
    listdir.return_value = "openlineage.yml"
    yaml.safe_load.return_value = {"transport": {"type": "accumulating"}}

    factory = DefaultTransportFactory()
    factory.register_transport(
        "accumulating",
        clazz="tests.transport.AccumulatingTransport"
    )
    transport = factory.create()
    assert isinstance(transport, AccumulatingTransport)


@patch.dict(os.environ, {})
@patch('os.getcwd')
def test_factory_registers_transports_from_yaml(cwd):
    cwd.return_value = current_path

    factory = DefaultTransportFactory()
    factory.register_transport(
        "accumulating",
        clazz="tests.transport.AccumulatingTransport"
    )
    transport = factory.create()
    assert isinstance(transport, AccumulatingTransport)


@patch.dict(os.environ, {"OPENLINEAGE_CONFIG": "tests/config/config.yml"})
def test_factory_registers_transports_from_yaml_config():
    factory = DefaultTransportFactory()
    factory.register_transport(
        "fake",
        clazz="tests.transport.FakeTransport"
    )
    transport = factory.create()
    assert isinstance(transport, FakeTransport)

@patch.dict(os.environ, {"OPENLINEAGE_CONFIG": "tests/config/http.yml"})
def test_factory_configures_http_transport_from_yaml_config():
    factory = get_default_factory()
    transport = factory.create()
    assert isinstance(transport, HttpTransport)


def test_factory_registers_from_dict():
    factory = DefaultTransportFactory()
    factory.register_transport(
        "fake",
        clazz="tests.transport.FakeTransport"
    )
    factory.register_transport(
        "accumulating",
        clazz="tests.transport.AccumulatingTransport"
    )
    config = {
        "type": "accumulating"
    }
    transport = factory.create(config=config)
    assert isinstance(transport, AccumulatingTransport)


def test_automatically_registers_http_kafka():
    factory = get_default_factory()
    assert HttpTransport in factory.transports.values()
    assert KafkaTransport in factory.transports.values()


@patch('openlineage.client.transport.factory.yaml')
@patch('os.listdir')
@patch('os.path.join')
def test_transport_decorator_registers(join, listdir, yaml):
    listdir.return_value = "openlineage.yml"
    yaml.safe_load.return_value = {"transport": {"type": "fake"}}

    transport = get_default_factory().create()
    assert isinstance(transport, FakeTransport)


@pytest.mark.parametrize("env_var_value, should_be_noop", [
    ("true", True),
    ("True", True),
    ("TRUE", True),
    ("false", False),
    ("False", False),
    ("FALSE", False),
])
def test_env_disables_client(env_var_value, should_be_noop):
    with patch.dict(os.environ, {"OPENLINEAGE_DISABLED": env_var_value}):
        transport = get_default_factory().create()
        is_noop = isinstance(transport, NoopTransport)
        assert is_noop is should_be_noop


@patch.dict(os.environ, {
    "OPENLINEAGE_DISABLED": "true",
    "OPENLINEAGE_CONFIG": "tests/config/config.yml"
})
def test_env_disabled_ignores_config():
    factory = DefaultTransportFactory()
    factory.register_transport(
        "fake",
        clazz="tests.transport.FakeTransport"
    )
    transport = factory.create()
    assert isinstance(transport, NoopTransport)
