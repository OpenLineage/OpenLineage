# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client.run import RunEvent
from openlineage.client.transport import Transport, Config, register_transport


class AccumulatingTransport(Transport):
    kind = "accumulating"
    config = Config

    def __init__(self, config: Config):
        self.events = []

    def emit(self, event: RunEvent):
        self.events.append(event)


@register_transport
class FakeTransport(Transport):
    kind = "fake"
    config = Config

    def __init__(self, config: Config):
        pass

    def emit(self, event: RunEvent):
        pass
