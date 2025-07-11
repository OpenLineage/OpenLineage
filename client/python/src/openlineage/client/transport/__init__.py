# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from openlineage.client.transport.amazon_datazone import AmazonDataZoneConfig, AmazonDataZoneTransport
from openlineage.client.transport.async_http import AsyncHttpConfig, AsyncHttpTransport
from openlineage.client.transport.composite import CompositeTransport
from openlineage.client.transport.console import ConsoleTransport
from openlineage.client.transport.datadog import DatadogConfig, DatadogTransport
from openlineage.client.transport.factory import DefaultTransportFactory
from openlineage.client.transport.file import FileTransport
from openlineage.client.transport.gcplineage import GCPLineageConfig, GCPLineageTransport
from openlineage.client.transport.http import HttpConfig, HttpTransport
from openlineage.client.transport.kafka import KafkaConfig, KafkaTransport
from openlineage.client.transport.msk_iam import MSKIAMConfig, MSKIAMTransport
from openlineage.client.transport.noop import NoopTransport
from openlineage.client.transport.transform.transform import (
    TransformConfig,
    TransformTransport,
)
from openlineage.client.transport.transport import Config, Transport, TransportFactory

_factory = DefaultTransportFactory()
_factory.register_transport(CompositeTransport.kind, CompositeTransport)
_factory.register_transport(HttpTransport.kind, HttpTransport)
_factory.register_transport(AsyncHttpTransport.kind, AsyncHttpTransport)
_factory.register_transport(KafkaTransport.kind, KafkaTransport)
_factory.register_transport(MSKIAMTransport.kind, MSKIAMTransport)
_factory.register_transport(ConsoleTransport.kind, ConsoleTransport)
_factory.register_transport(NoopTransport.kind, NoopTransport)
_factory.register_transport(FileTransport.kind, FileTransport)
_factory.register_transport(TransformTransport.kind, TransformTransport)
_factory.register_transport(AmazonDataZoneTransport.kind, AmazonDataZoneTransport)
_factory.register_transport(DatadogTransport.kind, DatadogTransport)
_factory.register_transport(GCPLineageTransport.kind, GCPLineageTransport)


def get_default_factory() -> DefaultTransportFactory:
    return _factory


# decorator to wrap transports with
def register_transport(clazz: type[Transport]) -> type[Transport]:
    assert clazz.kind is not None
    _factory.register_transport(clazz.kind, clazz)
    return clazz


__all__ = [
    "AmazonDataZoneConfig",
    "AmazonDataZoneTransport",
    "AsyncHttpConfig",
    "AsyncHttpTransport",
    "CompositeTransport",
    "Config",
    "ConsoleTransport",
    "DatadogConfig",
    "DatadogTransport",
    "FileTransport",
    "GCPLineageConfig",
    "GCPLineageTransport",
    "HttpConfig",
    "HttpTransport",
    "KafkaConfig",
    "KafkaTransport",
    "MSKIAMConfig",
    "MSKIAMTransport",
    "NoopTransport",
    "TransformConfig",
    "TransformTransport",
    "Transport",
    "TransportFactory",
    "get_default_factory",
    "register_transport",
]
