import os
from unittest.mock import patch

import pytest

from openlineage.client import OpenLineageClient
from openlineage.client.transport import DefaultTransportFactory, \
    get_default_factory, KafkaTransport
from openlineage.client.transport.http import HttpTransport
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
