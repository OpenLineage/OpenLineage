# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum
from typing import Any, ClassVar, cast

from attr import define, fields, make_class
from attr import field as attr_field
from openlineage.client.constants import DEFAULT_PRODUCER
from openlineage.client.utils import RedactMixin

PRODUCER = DEFAULT_PRODUCER


def set_producer(producer: str) -> None:
    global PRODUCER  # noqa: PLW0603
    PRODUCER = producer


@define(kw_only=True)
class BaseEvent(RedactMixin):
    eventTime: str = attr_field()  # noqa: N815
    """the time the event occurred at"""

    producer: str = attr_field(default="", kw_only=True)  # noqa: N815
    schemaURL: str = attr_field(  # noqa: N815
        default="https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseEvent", init=False
    )
    _base_skip_redact: ClassVar[list[str]] = ["producer", "schemaURL"]
    _additional_skip_redact: ClassVar[list[str]] = []

    def __attrs_post_init__(self) -> None:
        if not self.producer:
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

    @producer.validator
    def producer_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from urllib.parse import urlparse

        urlparse(value)

    @schemaURL.validator
    def schemaurl_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from urllib.parse import urlparse

        urlparse(value)


@define
class BaseFacet(RedactMixin):
    """all fields of the base facet are prefixed with _ to avoid name conflicts in facets"""

    _producer: str = attr_field(default="", kw_only=True)  # noqa: N815
    _schemaURL: str = attr_field(  # noqa: N815
        default="https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet", init=False
    )
    _base_skip_redact: ClassVar[list[str]] = ["_producer", "_schemaURL"]
    _additional_skip_redact: ClassVar[list[str]] = []

    def __attrs_post_init__(self) -> None:
        if not self._producer:
            self._producer = PRODUCER
        self._schemaURL = self._get_schema()

    @property
    def skip_redact(self) -> list[str]:
        return self._base_skip_redact + self._additional_skip_redact

    def with_additional_properties(self, **kwargs: dict[str, Any]) -> "BaseFacet":
        """Add additional properties to updated class instance."""
        current_attrs = [a.name for a in fields(self.__class__)]

        new_class = make_class(
            self.__class__.__name__,
            {k: attr_field(default=None) for k in kwargs if k not in current_attrs},
            bases=(self.__class__,),
        )
        new_class.__module__ = self.__class__.__module__
        attrs = fields(self.__class__)
        for a in attrs:
            if not a.init:
                continue
            attr_name = a.name  # To deal with private attributes.
            init_name = a.alias
            if init_name not in kwargs:
                kwargs[init_name] = getattr(self, attr_name)
        return cast(BaseFacet, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet"

    @_producer.validator
    def _producer_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from urllib.parse import urlparse

        urlparse(value)

    @_schemaURL.validator
    def _schemaurl_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from urllib.parse import urlparse

        urlparse(value)


@define
class Dataset(RedactMixin):
    namespace: str
    """The namespace containing that dataset"""

    name: str
    """The unique name for that dataset within that namespace"""

    facets: dict[str, DatasetFacet] | None = attr_field(factory=dict, kw_only=True)
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

    _deleted: bool | None = attr_field(default=None, kw_only=True)
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

    inputFacets: dict[str, InputDatasetFacet] | None = attr_field(factory=dict)  # noqa: N815
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

    facets: dict[str, JobFacet] | None = attr_field(factory=dict)
    """The job facets."""

    _skip_redact: ClassVar[list[str]] = ["namespace", "name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/Job"


@define(kw_only=True)
class JobEvent(BaseEvent):
    job: Job
    inputs: list[InputDataset] | None = attr_field(factory=list)
    """The set of **input** datasets."""

    outputs: list[OutputDataset] | None = attr_field(factory=list)
    """The set of **output** datasets."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent"


@define
class JobFacet(BaseFacet):
    """A Job Facet"""

    _deleted: bool | None = attr_field(default=None, kw_only=True)
    """set to true to delete a facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobFacet"


@define
class OutputDataset(Dataset):
    """An output dataset"""

    outputFacets: dict[str, OutputDatasetFacet] | None = attr_field(factory=dict)  # noqa: N815
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
    runId: str = attr_field()  # noqa: N815
    """The globally unique ID of the run associated with the job."""

    facets: dict[str, RunFacet] | None = attr_field(factory=dict)
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
    eventType: EventType | None = attr_field(default=None)  # noqa: N815
    """
    the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE,
    ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run.
    For example to send additional metadata after the run is complete
    """
    inputs: list[InputDataset] | None = attr_field(factory=list)
    """The set of **input** datasets."""

    outputs: list[OutputDataset] | None = attr_field(factory=list)
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
