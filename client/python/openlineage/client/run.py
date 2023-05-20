# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import uuid
from enum import Enum
from typing import Any

import attr
from dateutil import parser

from openlineage.client.facet import NominalTimeRunFacet, ParentRunFacet
from openlineage.client.utils import RedactMixin


class RunState(Enum):
    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ABORT = "ABORT"
    FAIL = "FAIL"
    OTHER = "OTHER"


_RUN_FACETS = [
    NominalTimeRunFacet,
    ParentRunFacet,
]


@attr.s
class Run(RedactMixin):
    runId: str = attr.ib()  # noqa:  N815
    facets: dict[Any, Any] = attr.ib(factory=dict)

    _skip_redact: list[str] = ["runId"]

    @runId.validator
    def check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        uuid.UUID(value)


@attr.s
class Job(RedactMixin):
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: dict[Any, Any] = attr.ib(factory=dict)

    _skip_redact: list[str] = ["namespace", "name"]


@attr.s
class Dataset(RedactMixin):
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: dict[Any, Any] = attr.ib(factory=dict)

    _skip_redact: list[str] = ["namespace", "name"]


@attr.s
class InputDataset(Dataset):
    inputFacets: dict[Any, Any] = attr.ib(factory=dict)  # noqa:  N815


@attr.s
class OutputDataset(Dataset):
    outputFacets: dict[Any, Any] = attr.ib(factory=dict)  # noqa:  N815


@attr.s
class RunEvent(RedactMixin):
    eventType: RunState = attr.ib(validator=attr.validators.in_(RunState))  # noqa:  N815
    eventTime: str = attr.ib()  # noqa:  N815
    run: Run = attr.ib()
    job: Job = attr.ib()
    producer: str = attr.ib()
    inputs: list[Dataset] | None = attr.ib(factory=list)  # type: ignore[assignment]
    outputs: list[Dataset] | None = attr.ib(factory=list)  # type: ignore[assignment]

    _skip_redact: list[str] = ["eventType", "eventTime", "producer"]

    @eventTime.validator
    def check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)
