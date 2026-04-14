# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, cast

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class LineageDatasetFacet(DatasetFacet):
    inputs: list[LineageDatasetInput] | None = attr.field(factory=list)
    """
    Dataset-level source inputs that feed into this dataset. When a source includes a 'field' property,
    it represents a dataset-wide operation (e.g., GROUP BY, FILTER) where that source column affects the
    entire target dataset.
    """
    fields: dict[str, LineageDatasetFieldEntry] | None = attr.field(factory=dict)
    """Column-level lineage. Maps target field names in this dataset to their source inputs."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageDatasetFacet.json#/$defs/LineageDatasetFacet"


@attr.define
class LineageDatasetFieldEntry(RedactMixin):
    """Column-level lineage for a single target field."""

    inputs: list[LineageDatasetInput]
    """Source entities and/or fields that feed into this target field."""

    def with_additional_properties(self, **kwargs: Any) -> "LineageDatasetFieldEntry":
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
        return cast(LineageDatasetFieldEntry, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageDatasetFacet.json#/$defs/LineageDatasetFieldEntry"


@attr.define
class LineageDatasetInput(RedactMixin):
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
    transformations: list[LineageDatasetTransformation] | None = attr.field(factory=list)
    """Transformations applied to the source data."""

    def with_additional_properties(self, **kwargs: Any) -> "LineageDatasetInput":
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
        return cast(LineageDatasetInput, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageDatasetFacet.json#/$defs/LineageDatasetInput"


@attr.define
class LineageDatasetTransformation(RedactMixin):
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

    def with_additional_properties(self, **kwargs: Any) -> "LineageDatasetTransformation":
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
        return cast(LineageDatasetTransformation, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/LineageDatasetFacet.json#/$defs/LineageDatasetTransformation"
