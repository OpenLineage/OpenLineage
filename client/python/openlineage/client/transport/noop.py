# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from openlineage.client.transport.transport import Config, Transport

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)


class NoopConfig(Config):
    ...


class NoopTransport(Transport):
    kind = "noop"
    config_class = NoopConfig

    def __init__(self, config: NoopConfig) -> None:  # noqa: ARG002
        log.debug("Constructing OpenLineage transport that will NOT send any events.")

    def emit(self, event: Event) -> None:  # noqa: ARG002
        return None
