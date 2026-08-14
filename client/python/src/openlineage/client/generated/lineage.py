# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar, Literal, Union

import attr
from openlineage.client.generated.base import DatasetFacet, JobFacet
from openlineage.client.utils import RedactMixin


@attr.define
class LineageDatasetEntry(RedactMixin):
    """
    Describes data flowing into a target dataset from source entities, at entity and/or field
    granularity.
    """

    namespace: str
    """The namespace of the target dataset."""

    name: str
    """The name of the target dataset."""

    type: Literal["DATASET"]  # noqa: A003
    """The target entity type. Must be DATASET."""

    inputs: list[LineageInput] | None = attr.field(default=None)
    """
    Entity-level inputs feeding this target dataset. An empty array explicitly means that the target has
    no tracked upstream source.
    """
    fields: dict[str, LineageFieldEntry] | None = attr.field(factory=dict)
    """Field-level lineage. Maps target field names to their source inputs."""

    _skip_redact: ClassVar[list[str]] = ["namespace", "name", "type"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageDatasetEntry"


@attr.define
class LineageDatasetFacet(DatasetFacet):
    """
    Explicit lineage for this dataset. Describes which source entities feed into it, at entity and/or
    field granularity.
    """

    inputs: list[LineageInput] | None = attr.field(default=None)
    """
    Dataset-level source inputs. A source with a field represents a dataset-wide operation where that
    field affects the entire target dataset.
    """
    fields: dict[str, LineageFieldEntry] | None = attr.field(factory=dict)
    """Field-level lineage. Maps target field names to their source inputs."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageDatasetFacet"


@attr.define
class LineageDatasetInput(RedactMixin):
    """A source dataset that feeds data into a lineage target."""

    namespace: str
    """The namespace of the source dataset."""

    name: str
    """The name of the source dataset."""

    type: Literal["DATASET"]  # noqa: A003
    """The source entity type. Must be DATASET."""

    field: str | None = attr.field(default=None)
    """
    The source field. At entity level it represents a dataset-wide dependency; at field level it
    identifies the source field.
    """
    transformations: list[LineageTransformation] | None = attr.field(factory=list)
    """Transformations applied to the source data."""

    _skip_redact: ClassVar[list[str]] = ["namespace", "name", "type", "field"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageDatasetInput"


LineageEntry = Union["LineageDatasetEntry", "LineageJobEntry"]


@attr.define
class LineageFieldEntry(RedactMixin):
    """Field-level lineage for a single target field."""

    inputs: list[LineageInput]
    """Source entities and/or fields that feed into this target field."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageFieldEntry"


LineageInput = Union["LineageDatasetInput", "LineageJobInput"]


@attr.define
class LineageJobEntry(RedactMixin):
    """
    Describes data flowing into a target job. Without namespace and name, the target is the event's own
    job.
    """

    type: Literal["JOB"]  # noqa: A003
    """The target entity type. Must be JOB."""

    namespace: str | None = attr.field(default=None)
    """The namespace of the target job. Omit together with name to refer to the event's own job."""

    name: str | None = attr.field(default=None)
    """The name of the target job. Omit together with namespace to refer to the event's own job."""

    runId: str | None = attr.field(default=None)  # noqa: N815
    """The target job run when the relationship is tied to a specific execution."""

    inputs: list[LineageInput] | None = attr.field(factory=list)
    """Source inputs feeding this target job."""

    _skip_redact: ClassVar[list[str]] = ["namespace", "name", "type", "runId"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageJobEntry"

    @runId.validator
    def runid_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        if value is None:
            return
        from uuid import UUID

        UUID(value)


@attr.define
class LineageJobFacet(JobFacet):
    """
    Explicit lineage for a job. On a JobEvent it is the job's declared lineage; on a RunEvent it is
    lineage observed during that run.
    """

    entries: list[LineageEntry]
    """Lineage entries describing target entities and the source entities that feed them."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageJobFacet"


@attr.define
class LineageJobInput(RedactMixin):
    """
    A source job that feeds data into a lineage target. Without namespace and name, the source is the
    event's own job.
    """

    type: Literal["JOB"]  # noqa: A003
    """The source entity type. Must be JOB."""

    namespace: str | None = attr.field(default=None)
    """The namespace of the source job. Omit together with name to refer to the event's own job."""

    name: str | None = attr.field(default=None)
    """The name of the source job. Omit together with namespace to refer to the event's own job."""

    runId: str | None = attr.field(default=None)  # noqa: N815
    """The source job run when the relationship is tied to a specific execution."""

    transformations: list[LineageTransformation] | None = attr.field(factory=list)
    """Transformations applied by the source job to produce the data."""

    _skip_redact: ClassVar[list[str]] = ["namespace", "name", "type", "runId"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageJobInput"

    @runId.validator
    def runid_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        if value is None:
            return
        from uuid import UUID

        UUID(value)


@attr.define
class LineageTransformation(RedactMixin):
    """A transformation applied to source data in a lineage relationship."""

    type: str  # noqa: A003
    """The transformation type, such as DIRECT or INDIRECT."""

    subtype: str | None = attr.field(default=None)
    """
    The transformation subtype, such as IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW, SORT, or
    CONDITIONAL.
    """
    description: str | None = attr.field(default=None)
    """A string representation of the transformation."""

    masking: bool | None = attr.field(default=None)
    """Whether the transformation masks the source data."""

    _skip_redact: ClassVar[list[str]] = ["type", "subtype", "masking"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageTransformation"
