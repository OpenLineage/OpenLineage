# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# flake8: noqa
from typing import Type

from openlineage.client.transport.noop import NoopTransport
from openlineage.client.transport.transport import Transport, Config, TransportFactory
from openlineage.client.transport.factory import DefaultTransportFactory
from openlineage.client.transport.http import HttpTransport, HttpConfig
from openlineage.client.transport.kafka import KafkaTransport, KafkaConfig
from openlineage.client.transport.console import ConsoleTransport


_factory = DefaultTransportFactory()
_factory.register_transport(HttpTransport.kind, HttpTransport)
_factory.register_transport(KafkaTransport.kind, KafkaTransport)
_factory.register_transport(ConsoleTransport.kind, ConsoleTransport)
_factory.register_transport(NoopTransport.kind, NoopTransport)


def get_default_factory():
    return _factory


# decorator to wrap transports with
def register_transport(clazz: Type[Transport]):
    _factory.register_transport(clazz.kind, clazz)
    return clazz
