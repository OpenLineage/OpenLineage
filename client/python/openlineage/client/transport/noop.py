# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union

from openlineage.client.transport.transport import Config, Transport

if TYPE_CHECKING:
    from openlineage.client.run import DatasetEvent, JobEvent, RunEvent

log = logging.getLogger(__name__)


class NoopConfig(Config):
    ...


class NoopTransport(Transport):
    kind = "noop"
    config_class = NoopConfig

    def __init__(self, config: NoopConfig) -> None:  # noqa: ARG002
        log.info("OpenLineage client is disabled. NoopTransport.")

    def emit(self, event: Union[RunEvent, DatasetEvent, JobEvent]) -> None:  # noqa: ARG002, UP007
        return None
