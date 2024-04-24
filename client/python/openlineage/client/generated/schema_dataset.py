# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Optional

from attr import define, field
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@define
class SchemaDatasetFacet(DatasetFacet):
    fields: Optional[list[SchemaDatasetFacetFields]] = field(factory=list)  # type: ignore[assignment]
    """The fields of the data source."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"


@define
class SchemaDatasetFacetFields(RedactMixin):
    name: str
    """The name of the field."""

    type: Optional[str] = field(default=None)
    """The type of the field."""

    description: Optional[str] = field(default=None)
    """The description of the field."""

    fields: Optional[list[SchemaDatasetFacetFields]] = field(factory=list)  # type: ignore[assignment]
    """Nested struct fields."""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacetFields"
        )
