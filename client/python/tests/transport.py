# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

from openlineage.client.transport import Config, Transport, register_transport

if TYPE_CHECKING:
    from openlineage.client.run import RunEvent


class AccumulatingTransport(Transport):
    kind = "accumulating"
    config = Config

    def __init__(self, config: Config) -> None:  # noqa: ARG002
        self.events = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)


@register_transport
class FakeTransport(Transport):
    kind = "fake"
    config = Config

    def __init__(self, config: Config) -> None:
        ...

    def emit(self, event: RunEvent) -> None:
        ...
