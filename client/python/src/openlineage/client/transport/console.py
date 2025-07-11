# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)


class ConsoleConfig(Config): ...


class ConsoleTransport(Transport):
    kind = "console"
    config_class = ConsoleConfig

    def __init__(self, config: ConsoleConfig) -> None:  # noqa: ARG002
        log.debug("Constructing OpenLineage transport that will send events to console or logs")

    def emit(self, event: Event) -> None:
        # Note: When the logging level is set to DEBUG, the content of events is logged twice:
        # here on the INFO level and in client.py on the DEBUG level for when different transport is used.
        log.info(Serde.to_json(event))
