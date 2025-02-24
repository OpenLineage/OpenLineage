# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path
from typing import Union

import pytest
from openlineage.client import event_v2, set_producer
from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
from openlineage.client.transport.transport import Config, Transport


@pytest.fixture(scope="session")
def root() -> Path:
    return Path(__file__).parent


@pytest.fixture(scope="session", autouse=True)
def _setup_producer() -> None:
    set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python")


# For testing events emitted by the client

Event_v1 = Union[RunEvent, DatasetEvent, JobEvent]
Event_v2 = Union[event_v2.RunEvent, event_v2.DatasetEvent, event_v2.JobEvent]
Event = Union[Event_v1, Event_v2]


class NoOutputConfig(Config):
    ...


class NoOutputTransport(Transport):
    """Minimal transport that stores the event for assertions"""

    kind = "no_output"
    config_class = NoOutputConfig

    def __init__(self, config: NoOutputConfig) -> None:  # noqa: ARG002
        self._event = None

    def emit(self, event: Event) -> None:
        self._event = event

    @property
    def event(self):
        return self._event


@pytest.fixture(scope="session")
def transport():
    return NoOutputTransport(config=NoOutputConfig())
