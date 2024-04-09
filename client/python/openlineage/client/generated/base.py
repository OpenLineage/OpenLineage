# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum
from typing import ClassVar

from attr import define, field
from openlineage.client.constants import DEFAULT_PRODUCER
from openlineage.client.utils import RedactMixin


@define(kw_only=True)
class BaseEvent(RedactMixin):
    eventTime: str = field()  # noqa: N815
    """the time the event occurred at"""

    producer: str = field(default="", kw_only=True)
    schemaURL: str = field(  # noqa: N815
        default="https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseEvent", init=False
    )
    _base_skip_redact: ClassVar[list[str]] = ["producer", "schemaURL"]
    _additional_skip_redact: ClassVar[list[str]] = []

    def __attrs_post_init__(self) -> None:
        import warnings

        if self.producer:
            warnings.warn(
                "producer as argument is deprecated. Please use `set_producer` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            self.producer = PRODUCER
        self.schemaURL = self._get_schema()

    @property
    def skip_redact(self) -> list[str]:
        return self._base_skip_redact + self._additional_skip_redact

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseEvent"

    @eventTime.validator
    def eventtime_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from dateutil import parser

        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)


@define
class BaseFacet(RedactMixin):
    """all fields of the base facet are prefixed with _ to avoid name conflicts in facets"""

    _producer: str = field(default="", kw_only=True)
    _schemaURL: str = field(  # noqa: N815
        default="https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet", init=False
    )
    _base_skip_redact: ClassVar[list[str]] = ["_producer", "_schemaURL"]
    _additional_skip_redact: ClassVar[list[str]] = []

    def __attrs_post_init__(self) -> None:
        import warnings

        if self._producer:
            warnings.warn(
                "_producer as argument is deprecated. Please use `set_producer` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            self._producer = PRODUCER
        self._schemaURL = self._get_schema()

    @property
    def skip_redact(self) -> list[str]:
        return self._base_skip_redact + self._additional_skip_redact

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet"


@define
class Dataset(RedactMixin):
    namespace: str
    """The namespace containing that dataset"""

    name: str
    """The unique name for that dataset within that namespace"""

    facets: dict[str, DatasetFacet] | None = field(factory=dict, kw_only=True)  # type: ignore[assignment]
    """The facets for this dataset"""

    _skip_redact: ClassVar[list[str]] = ["namespace", "name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/Dataset"


@define(kw_only=True)
class DatasetEvent(BaseEvent):
    dataset: StaticDataset

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent"


@define
class DatasetFacet(BaseFacet):
    """A Dataset Facet"""

    _deleted: bool | None = field(default=None, kw_only=True)
    """set to true to delete a facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetFacet"


class EventType(Enum):
    """
    the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE,
    ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run.
    For example to send additional metadata after the run is complete
    """

    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ABORT = "ABORT"
    FAIL = "FAIL"
    OTHER = "OTHER"


@define
class InputDataset(Dataset):
    """An input dataset"""

    inputFacets: dict[str, InputDatasetFacet] | None = field(factory=dict)  # type: ignore[assignment]# noqa: N815
    """The input facets for this dataset."""

    _additional_skip_redact: ClassVar[list[str]] = ["namespace", "name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/InputDataset"


@define
class InputDatasetFacet(BaseFacet):
    """An Input Dataset Facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/InputDatasetFacet"


@define
class Job(RedactMixin):
    namespace: str
    """The namespace containing that job"""

    name: str
    """The unique name for that job within that namespace"""

    facets: dict[str, JobFacet] | None = field(factory=dict)  # type: ignore[assignment]
    """The job facets."""

    _skip_redact: ClassVar[list[str]] = ["namespace", "name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/Job"


@define(kw_only=True)
class JobEvent(BaseEvent):
    job: Job
    inputs: list[InputDataset] | None = field(factory=list)  # type: ignore[assignment]
    """The set of **input** datasets."""

    outputs: list[OutputDataset] | None = field(factory=list)  # type: ignore[assignment]
    """The set of **output** datasets."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent"


@define
class JobFacet(BaseFacet):
    """A Job Facet"""

    _deleted: bool | None = field(default=None, kw_only=True)
    """set to true to delete a facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobFacet"


@define
class OutputDataset(Dataset):
    """An output dataset"""

    outputFacets: dict[str, OutputDatasetFacet] | None = field(factory=dict)  # type: ignore[assignment]# noqa: N815
    """The output facets for this dataset"""

    _additional_skip_redact: ClassVar[list[str]] = ["namespace", "name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/OutputDataset"


@define
class OutputDatasetFacet(BaseFacet):
    """An Output Dataset Facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/OutputDatasetFacet"


@define
class Run(RedactMixin):
    runId: str = field()  # noqa: N815
    """The globally unique ID of the run associated with the job."""

    facets: dict[str, RunFacet] | None = field(factory=dict)  # type: ignore[assignment]
    """The run facets."""

    _skip_redact: ClassVar[list[str]] = ["runId"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/Run"

    @runId.validator
    def runid_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from uuid import UUID

        UUID(value)


@define(kw_only=True)
class RunEvent(BaseEvent):
    run: Run
    job: Job
    eventType: EventType | None = field(default=None)  # noqa: N815
    """
    the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE,
    ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run.
    For example to send additional metadata after the run is complete
    """
    inputs: list[InputDataset] | None = field(factory=list)  # type: ignore[assignment]
    """The set of **input** datasets."""

    outputs: list[OutputDataset] | None = field(factory=list)  # type: ignore[assignment]
    """The set of **output** datasets."""

    _additional_skip_redact: ClassVar[list[str]] = ["eventType", "eventTime"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"


@define
class RunFacet(BaseFacet):
    """A Run Facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet"


@define
class StaticDataset(Dataset):
    """A Dataset sent within static metadata events"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/StaticDataset"


PRODUCER = DEFAULT_PRODUCER


def set_producer(producer: str) -> None:
    global PRODUCER  # noqa: PLW0603
    PRODUCER = producer
