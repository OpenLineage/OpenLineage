# SPDX-License-Identifier: Apache-2.0.

from typing import Dict, List, Optional
from enum import Enum

import uuid
import attr

from openlineage.client.facet import NominalTimeRunFacet, ParentRunFacet


class RunState(Enum):
    START = 'START'
    COMPLETE = 'COMPLETE'
    ABORT = 'ABORT'
    FAIL = 'FAIL'
    OTHER = 'OTHER'


_RUN_FACETS = [
    NominalTimeRunFacet,
    ParentRunFacet
]


@attr.s
class Run:
    runId: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)

    @runId.validator
    def check(self, attribute, value):
        uuid.UUID(value)


@attr.s
class Job:
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)


@attr.s
class Dataset:
    namespace: str = attr.ib()
    name: str = attr.ib()
    facets: Dict = attr.ib(factory=dict)


@attr.s
class InputDataset(Dataset):
    inputFacets: Dict = attr.ib(factory=dict)


@attr.s
class OutputDataset(Dataset):
    outputFacets: Dict = attr.ib(factory=dict)


@attr.s
class RunEvent:
    eventType: RunState = attr.ib(validator=attr.validators.in_(RunState))
    eventTime: str = attr.ib()  # TODO: validate dates
    run: Run = attr.ib()
    job: Job = attr.ib()
    producer: str = attr.ib()
    inputs: Optional[List[Dataset]] = attr.ib(factory=list, type=Dataset)
    outputs: Optional[List[Dataset]] = attr.ib(factory=list, type=Dataset)
