# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging

from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport


class ConsoleConfig(Config):
    pass


class ConsoleTransport(Transport):
    kind = 'console'
    config = ConsoleConfig

    def __init__(self, config: ConsoleConfig):
        self.log = logging.getLogger(__name__)
        self.log.debug("Constructing openlineage client to send events to console or logs")

    def emit(self, event: RunEvent):
        # If logging is set to DEBUG, this will also log event in client.py
        self.log.info(Serde.to_json(event))
