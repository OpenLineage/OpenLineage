# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import uuid
from enum import Enum
from typing import Dict, List, Optional

import attr
from dateutil import parser
from openlineage.client.facet import NominalTimeRunFacet, ParentRunFacet
from openlineage.client.utils import RedactMixin


class RunState(Enum):
    START = 'START'
    RUNNING = 'RUNNING'
    COMPLETE = 'COMPLETE'
    ABORT = 'ABORT'
    FAIL = 'FAIL'
    OTHER = 'OTHER'


_RUN_FACETS = [
    NominalTimeRunFacet,
    ParentRunFacet
]


@attr.s
class Run(RedactMixin):
    runId: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)

    _skip_redact: List[str] = ['runId']

    @runId.validator
    def check(self, attribute, value):
        uuid.UUID(value)


@attr.s
class Job(RedactMixin):
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)

    _skip_redact: List[str] = ['namespace', 'name']


@attr.s
class Dataset(RedactMixin):
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)

    _skip_redact: List[str] = ['namespace', 'name']


@attr.s
class InputDataset(Dataset):
    inputFacets: Dict = attr.ib(factory=dict)


@attr.s
class OutputDataset(Dataset):
    outputFacets: Dict = attr.ib(factory=dict)


@attr.s
class RunEvent(RedactMixin):
    eventType: RunState = attr.ib(validator=attr.validators.in_(RunState))
    eventTime: str = attr.ib()
    run: Run = attr.ib()
    job: Job = attr.ib()
    producer: str = attr.ib()
    inputs: Optional[List[Dataset]] = attr.ib(factory=list)     # type: ignore
    outputs: Optional[List[Dataset]] = attr.ib(factory=list)    # type: ignore

    _skip_redact: List[str] = ['eventType', 'eventTime', 'producer']

    @eventTime.validator
    def check(self, attribute, value):
        parser.isoparse(value)
        if 't' not in value.lower():
            # make sure date-time contains time
            raise ValueError("Parsed date-time has to contain time: {}".format(value))
