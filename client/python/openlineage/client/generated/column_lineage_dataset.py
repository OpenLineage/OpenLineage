# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, ClassVar, cast

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class ColumnLineageDatasetFacet(DatasetFacet):
    fields: dict[str, Fields]
    """Column level lineage that maps output fields into input fields used to evaluate them."""

    dataset: list[InputField] | None = attr.field(factory=list)
    """
    Column level lineage that affects the whole dataset. This includes filtering, sorting, grouping
    (aggregates), joining, window functions, etc.
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet"


@attr.define
class Fields(RedactMixin):
    inputFields: list[InputField]  # noqa: N815
    transformationDescription: str | None = attr.field(default=None)  # noqa: N815
    """a string representation of the transformation applied"""

    transformationType: str | None = attr.field(default=None)  # noqa: N815
    """
    IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input; MASKED: no
    original data available (like a hash of PII for example)
    """

    def with_additional_properties(self, **kwargs: dict[str, Any]) -> "Fields":
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
        return cast(Fields, new_class(**kwargs))


@attr.define
class InputField(RedactMixin):
    """Represents a single dependency on some field (column)."""

    namespace: str
    """The input dataset namespace"""

    name: str
    """The input dataset name"""

    field: str
    """The input field"""

    transformations: list[Transformation] | None = attr.field(factory=list)
    _skip_redact: ClassVar[list[str]] = ["namespace", "name", "field"]

    def with_additional_properties(self, **kwargs: dict[str, Any]) -> "InputField":
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
        return cast(InputField, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/InputField"


@attr.define
class Transformation(RedactMixin):
    type: str  # noqa: A003
    """The type of the transformation. Allowed values are: DIRECT, INDIRECT"""

    subtype: str | None = attr.field(default=None)
    """The subtype of the transformation"""

    description: str | None = attr.field(default=None)
    """a string representation of the transformation applied"""

    masking: bool | None = attr.field(default=None)
    """is transformation masking the data or not"""

    _skip_redact: ClassVar[list[str]] = ["type", "subtype", "masking"]

    def with_additional_properties(self, **kwargs: dict[str, Any]) -> "Transformation":
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
        return cast(Transformation, new_class(**kwargs))
