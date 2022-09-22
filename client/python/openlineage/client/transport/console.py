# SPDX-License-Identifier: Apache-2.0.
import logging

from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Transport, Config


class ConsoleConfig(Config):
    pass


class ConsoleTransport(Transport):
    kind = 'console'
    config = ConsoleConfig

    def __init__(self, config: ConsoleConfig):
        self.log = logging.getLogger(__name__)
        self.log.debug("Constructing openlineage client to send events to console or logs")

    def emit(self, event: RunEvent):
        self.log.info(Serde.to_json(event))
