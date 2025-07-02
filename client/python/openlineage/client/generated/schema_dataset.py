# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class SchemaDatasetFacet(DatasetFacet):
    fields: list[SchemaDatasetFacetFields] | None = attr.field(factory=list)
    """The fields of the data source."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"


@attr.define
class SchemaDatasetFacetFields(RedactMixin):
    name: str
    """The name of the field."""

    type: str | None = attr.field(default=None)  # noqa: A003
    """The type of the field."""

    description: str | None = attr.field(default=None)
    """The description of the field."""

    fields: list[SchemaDatasetFacetFields] | None = attr.field(factory=list)
    """Nested struct fields."""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacetFields"
        )
