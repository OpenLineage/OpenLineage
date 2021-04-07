import json
from typing import Dict, List, Optional
from enum import Enum

import attr

from openlineage.facet import NominalTimeRunFacet, ParentRunFacet


class EventEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        if obj is None:
            return
        return super().default(obj)


class Serde:
    @staticmethod
    def to_json(obj):
        serialized = attr.asdict(obj)
        without_nulls = {k: v for k, v in serialized.items() if v is not None}
        return json.dumps(without_nulls, cls=EventEncoder, sort_keys=True)


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
class RunEvent:
    eventType: RunState = attr.ib(validator=attr.validators.in_(RunState))
    eventTime: str = attr.ib()  # TODO: validate dates
    run: Run = attr.ib()
    job: Job = attr.ib()
    producer: str = attr.ib()
    inputs: Optional[List[Dataset]] = attr.ib(factory=list)
    outputs: Optional[List[Dataset]] = attr.ib(factory=list)
