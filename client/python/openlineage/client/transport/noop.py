# SPDX-License-Identifier: Apache-2.0.
import logging

from openlineage.client.run import RunEvent
from openlineage.client.transport.transport import Transport, Config


log = logging.getLogger(__name__)


class NoopConfig(Config):
    pass


class NoopTransport(Transport):
    kind = 'noop'
    config = NoopConfig

    def __init__(self, config: NoopConfig):
        log.info("OpenLineage client is disabled. NoopTransport.")

    def emit(self, event: RunEvent):
        pass
