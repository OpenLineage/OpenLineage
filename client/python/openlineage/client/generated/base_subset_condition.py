# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, cast

import attr
from openlineage.client.utils import RedactMixin


@attr.define
class BaseCompareExpression(RedactMixin):
    pass

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-0/BaseSubsetCondition.json#/$defs/BaseCompareExpression"
        )


@attr.define
class BaseSubsetCondition(RedactMixin):
    """The condition to define a subset"""

    type: str  # noqa: A003

    def with_additional_properties(self, **kwargs: dict[str, Any]) -> "BaseSubsetCondition":
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
        return cast(BaseSubsetCondition, new_class(**kwargs))

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetCondition.json#/$defs/BaseSubsetCondition"


@attr.define
class BinarySubsetCondition(BaseSubsetCondition):
    left: BaseSubsetCondition
    right: BaseSubsetCondition
    operator: str
    """Allowed values: 'AND' or 'OR'"""

    type: str = "binary"  # noqa: A003

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-0/BaseSubsetCondition.json#/$defs/BinarySubsetCondition"
        )


@attr.define
class CompareSubsetCondition(BaseSubsetCondition):
    type: str = "compare"  # noqa: A003
    left: BaseSubsetCondition | None = attr.field(default=None)
    right: BaseSubsetCondition | None = attr.field(default=None)
    comparison: str | None = attr.field(default=None)
    """Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN', 'LESS_EQUAL_THAN'"""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-0/BaseSubsetCondition.json#/$defs/CompareSubsetCondition"
        )


@attr.define
class FieldBaseCompareExpression(BaseCompareExpression):
    type: str = "field"  # noqa: A003
    field: str | None = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetCondition.json#/$defs/FieldBaseCompareExpression"


@attr.define
class LocationSubsetCondition(BaseSubsetCondition):
    locations: list[str]
    type: str = "location"  # noqa: A003

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-0/BaseSubsetCondition.json#/$defs/LocationSubsetCondition"
        )


@attr.define
class Partition(RedactMixin):
    identifier: str | None = attr.field(default=None)
    """Optionally provided identifier of the partition specified"""

    type: str = "partition"  # noqa: A003
    dimensions: dict[str, Any] | None = attr.field(factory=dict)


@attr.define
class PartitionSubsetCondition(BaseSubsetCondition):
    partitions: list[Partition] | None = attr.field(factory=list)

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetCondition.json#/$defs/PartitionSubsetCondition"
