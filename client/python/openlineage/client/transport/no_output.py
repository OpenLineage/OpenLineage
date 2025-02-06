# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from openlineage.client.transport.transport import Config, Transport

if TYPE_CHECKING:
    from openlineage.client.client import Event


class NoOutputConfig(Config):
    ...


class NoOutputTransport(Transport):
    """Used only for writing tests. Simply stores the event for assertions"""

    kind = "no_output"
    config_class = NoOutputConfig

    def __init__(self, config: NoOutputConfig) -> None:  # noqa: ARG002
        self.event = None
        self.log = logging.getLogger(__name__)
        self.log.debug("No output will be generated from this transport")

    def emit(self, event: Event) -> None:
        self.event = event
