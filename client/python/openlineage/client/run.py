# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    inputs: Optional[List[Dataset]] = attr.ib(factory=list)
    outputs: Optional[List[Dataset]] = attr.ib(factory=list)
