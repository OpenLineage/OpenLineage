# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union

from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport

if TYPE_CHECKING:
    from openlineage.client.run import DatasetEvent, JobEvent, RunEvent


class ConsoleConfig(Config):
    ...


class ConsoleTransport(Transport):
    kind = "console"
    config_class = ConsoleConfig

    def __init__(self, config: ConsoleConfig) -> None:  # noqa: ARG002
        self.log = logging.getLogger(__name__)
        self.log.debug("Constructing openlineage client to send events to console or logs")

    def emit(self, event: Union[RunEvent, DatasetEvent, JobEvent]) -> None:  # noqa: UP007
        # If logging is set to DEBUG, this will also log event in client.py
        self.log.info(Serde.to_json(event))
