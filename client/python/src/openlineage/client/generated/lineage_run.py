# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, cast

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class LineageEntry(RedactMixin):
    """
    Describes data flowing into a target entity from source entities, at entity and/or column
    granularity.
    """

    namespace: str
    """The namespace of the target entity."""

    name: str
    """The name of the target entity."""

    type: str  # noqa: A003
    """
    The type of the target entity. DATASET for dataset entities. JOB for job entities, used when the job
    itself is the data consumer (sink) or producer (generator) — i.e., when there are no output datasets
    (sink) or no input datasets (generator).
    """
    inputs: list[LineageInput] | None = attr.field(factory=list)
    """
    Entity-level source inputs. An empty array explicitly means the target has no upstream source (e.g.,
    a data generator).
    """
    fields: dict[str, LineageFieldEntry] | None = attr.field(factory=dict)
    """
    Column-level lineage. Maps target field names to their source inputs. Only meaningful when the
    target type is DATASET.
    """

    def with_additional_properties(self, **kwargs: Any) -> "LineageEntry":
        """Add additional properties to updated class instance."""
        current_attrs = [a.name for a in attr.fields(self.__class__)]

        new_class = attr.make_class(
            self.__class__.__name__,
            {k: attr.field(default=None) for k in kwargs if k not in current_attrs},
            bases=(self.__class__,),
        )
        new_class.__module__ = self.__class__.__module__
        attrs = attr.fields(self.__class__)
        for a in attrs:
            if not a.init:
                continue
            attr_name = a.name  # To deal with private attributes.
            init_name = a.alias
            if init_name not in kwargs:
                kwargs[init_name] = getattr(self, attr_name)
        return cast(LineageEntry, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageRunFacet.json#/$defs/LineageEntry"


@attr.define
class LineageFieldEntry(RedactMixin):
    """Column-level lineage for a single target field."""

    inputs: list[LineageInput]
    """Source entities and/or fields that feed into this target field."""

    def with_additional_properties(self, **kwargs: Any) -> "LineageFieldEntry":
        """Add additional properties to updated class instance."""
        current_attrs = [a.name for a in attr.fields(self.__class__)]

        new_class = attr.make_class(
            self.__class__.__name__,
            {k: attr.field(default=None) for k in kwargs if k not in current_attrs},
            bases=(self.__class__,),
        )
        new_class.__module__ = self.__class__.__module__
        attrs = attr.fields(self.__class__)
        for a in attrs:
            if not a.init:
                continue
            attr_name = a.name  # To deal with private attributes.
            init_name = a.alias
            if init_name not in kwargs:
                kwargs[init_name] = getattr(self, attr_name)
        return cast(LineageFieldEntry, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageRunFacet.json#/$defs/LineageFieldEntry"


@attr.define
class LineageInput(RedactMixin):
    """A source entity that feeds data into a lineage target."""

    namespace: str
    """The namespace of the source entity."""

    name: str
    """The name of the source entity."""

    type: str  # noqa: A003
    """
    The type of the source entity. DATASET for dataset entities. JOB for job entities, used when a job
    is the origin of data (e.g., a generator job that creates data without reading from any input
    dataset).
    """
    field: str | None = attr.field(default=None)
    """
    The specific field/column of the source dataset. When present at entity-level inputs, represents a
    dataset-wide operation (e.g., GROUP BY column). When present at field-level inputs, represents the
    source column that feeds into the target column.
    """
    transformations: list[LineageTransformation] | None = attr.field(factory=list)
    """Transformations applied to the source data."""

    def with_additional_properties(self, **kwargs: Any) -> "LineageInput":
        """Add additional properties to updated class instance."""
        current_attrs = [a.name for a in attr.fields(self.__class__)]

        new_class = attr.make_class(
            self.__class__.__name__,
            {k: attr.field(default=None) for k in kwargs if k not in current_attrs},
            bases=(self.__class__,),
        )
        new_class.__module__ = self.__class__.__module__
        attrs = attr.fields(self.__class__)
        for a in attrs:
            if not a.init:
                continue
            attr_name = a.name  # To deal with private attributes.
            init_name = a.alias
            if init_name not in kwargs:
                kwargs[init_name] = getattr(self, attr_name)
        return cast(LineageInput, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageRunFacet.json#/$defs/LineageInput"


@attr.define
class LineageRunFacet(RunFacet):
    lineage: list[LineageEntry]
    """
    Lineage entries describing data flow observed during this run. Each entry identifies a target entity
    and the sources that feed into it.
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageRunFacet.json#/$defs/LineageRunFacet"


@attr.define
class LineageTransformation(RedactMixin):
    """A transformation applied to source data in a lineage relationship."""

    type: str  # noqa: A003
    """The type of the transformation. Allowed values are: DIRECT, INDIRECT."""

    subtype: str | None = attr.field(default=None)
    """
    The subtype of the transformation, e.g., IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY, WINDOW,
    SORT, CONDITIONAL.
    """
    description: str | None = attr.field(default=None)
    """A string representation of the transformation applied."""

    masking: bool | None = attr.field(default=None)
    """Whether the transformation masks the data (e.g., hashing PII)."""

    def with_additional_properties(self, **kwargs: Any) -> "LineageTransformation":
        """Add additional properties to updated class instance."""
        current_attrs = [a.name for a in attr.fields(self.__class__)]

        new_class = attr.make_class(
            self.__class__.__name__,
            {k: attr.field(default=None) for k in kwargs if k not in current_attrs},
            bases=(self.__class__,),
        )
        new_class.__module__ = self.__class__.__module__
        attrs = attr.fields(self.__class__)
        for a in attrs:
            if not a.init:
                continue
            attr_name = a.name  # To deal with private attributes.
            init_name = a.alias
            if init_name not in kwargs:
                kwargs[init_name] = getattr(self, attr_name)
        return cast(LineageTransformation, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageRunFacet.json#/$defs/LineageTransformation"
