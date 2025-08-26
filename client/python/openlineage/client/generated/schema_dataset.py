# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from attr import field as attr_field
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@define
class SchemaDatasetFacet(DatasetFacet):
    fields: list[SchemaDatasetFacetFields] | None = attr_field(factory=list)
    """The fields of the data source."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"


@define
class SchemaDatasetFacetFields(RedactMixin):
    name: str
    """The name of the field."""

    type: str | None = attr_field(default=None)  # noqa: A003
    """The type of the field."""

    description: str | None = attr_field(default=None)
    """The description of the field."""

    fields: list[SchemaDatasetFacetFields] | None = attr_field(factory=list)
    """Nested struct fields."""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacetFields"
        )
