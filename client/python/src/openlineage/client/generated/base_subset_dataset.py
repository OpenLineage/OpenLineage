# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, Union

import attr
from openlineage.client.generated.base import InputDatasetFacet, OutputDatasetFacet
from openlineage.client.utils import RedactMixin

BaseSubsetCondition = Union[
    "LocationSubsetCondition", "PartitionSubsetCondition", "BinarySubsetCondition", "CompareSubsetCondition"
]


BaseSubsetDatasetFacet = Union["InputSubsetInputDatasetFacet", "OutputSubsetOutputDatasetFacet"]


@attr.define
class BinarySubsetCondition(RedactMixin):
    left: BaseSubsetCondition
    right: BaseSubsetCondition
    operator: str
    """
    Allowed values: 'AND' or 'OR'

    Example: AND
    """
    type: str = "binary"  # noqa: A003

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/BinarySubsetCondition"


@attr.define
class CompareSubsetCondition(RedactMixin):
    left: FieldBaseCompareExpression | LiteralCompareExpression
    right: FieldBaseCompareExpression | LiteralCompareExpression
    comparison: str
    """
    Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN', 'LESS_EQUAL_THAN'

    Example: EQUAL
    """
    type: str = "compare"  # noqa: A003

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/CompareSubsetCondition"


@attr.define
class FieldBaseCompareExpression(RedactMixin):
    field: str
    type: str = "field"  # noqa: A003

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/FieldBaseCompareExpression"


@attr.define
class InputSubsetInputDatasetFacet(InputDatasetFacet):
    inputCondition: BaseSubsetCondition  # noqa: N815

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/InputSubsetInputDatasetFacet"


@attr.define
class LiteralCompareExpression(RedactMixin):
    value: str
    type: str = "literal"  # noqa: A003

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/LiteralCompareExpression"


@attr.define
class LocationSubsetCondition(RedactMixin):
    locations: list[str]
    type: str = "location"  # noqa: A003

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/LocationSubsetCondition"


@attr.define
class OutputSubsetOutputDatasetFacet(OutputDatasetFacet):
    outputCondition: BaseSubsetCondition  # noqa: N815

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/OutputSubsetOutputDatasetFacet"


@attr.define
class Partition(RedactMixin):
    dimensions: dict[str, Any]
    identifier: str | None = attr.field(default=None)
    """Optionally provided identifier of the partition specified"""


@attr.define
class PartitionSubsetCondition(RedactMixin):
    partitions: list[Partition]
    type: str = "partition"  # noqa: A003

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/PartitionSubsetCondition"
