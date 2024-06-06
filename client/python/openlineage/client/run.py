# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import uuid
import warnings
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional

import attr
from dateutil import parser
from openlineage.client.facet import NominalTimeRunFacet, ParentRunFacet
from openlineage.client.utils import RedactMixin

warnings.warn(
    "This module is deprecated. Please use `openlineage.client.event_v2`.", DeprecationWarning, stacklevel=2
)

SCHEMA_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"


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
class Dataset(RedactMixin):
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: Dict[Any, Any] = attr.ib(factory=dict)

    _skip_redact: ClassVar[List[str]] = ["namespace", "name"]


@attr.s
class InputDataset(Dataset):
    inputFacets: Dict[Any, Any] = attr.ib(factory=dict)  # noqa:  N815


@attr.s
class OutputDataset(Dataset):
    outputFacets: Dict[Any, Any] = attr.ib(factory=dict)  # noqa:  N815


@attr.s
class DatasetEvent(RedactMixin):
    eventTime: str = attr.ib()  # noqa:  N815
    producer: str = attr.ib()
    schemaURL: str = attr.ib()  # noqa: N815
    dataset: Dataset = attr.ib()

    _skip_redact: ClassVar[List[str]] = ["producer"]


@attr.s
class Job(RedactMixin):
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: Dict[Any, Any] = attr.ib(factory=dict)

    _skip_redact: ClassVar[List[str]] = ["namespace", "name"]


@attr.s
class JobEvent(RedactMixin):
    eventTime: str = attr.ib()  # noqa:  N815
    producer: str = attr.ib()
    schemaURL: str = attr.ib()  # noqa: N815
    job: Job = attr.ib()
    inputs: Optional[List[Dataset]] = attr.ib(factory=list)
    outputs: Optional[List[Dataset]] = attr.ib(factory=list)

    _skip_redact: ClassVar[List[str]] = ["producer"]


@attr.s
class Run(RedactMixin):
    runId: str = attr.ib()  # noqa:  N815
    facets: Dict[Any, Any] = attr.ib(factory=dict)

    _skip_redact: ClassVar[List[str]] = ["runId"]

    @runId.validator
    def check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        uuid.UUID(value)


@attr.s
class RunEvent(RedactMixin):
    eventType: RunState = attr.ib(validator=attr.validators.in_(RunState))  # noqa:  N815
    eventTime: str = attr.ib()  # noqa:  N815
    run: Run = attr.ib()
    job: Job = attr.ib()
    producer: str = attr.ib()
    inputs: Optional[List[Dataset]] = attr.ib(factory=list)
    outputs: Optional[List[Dataset]] = attr.ib(factory=list)
    schemaURL: str = attr.ib(default=SCHEMA_URL)  # noqa: N815

    _skip_redact: ClassVar[List[str]] = ["eventType", "eventTime", "producer", "schemaURL"]

    @eventTime.validator
    def check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)
