# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from typing import List, Optional
from openlineage.client.utils import RedactMixin
from attr import define, field
from openlineage.client import utils
from openlineage.client.generated.base import DatasetFacet


@define
class SchemaDatasetFacetFields(RedactMixin):
    name: str
    """The name of the field."""

    type: Optional[str] = field(default=None)  # noqa: A003
    """The type of the field."""

    description: Optional[str] = field(default=None)
    """The description of the field."""

    fields: Optional[List[SchemaDatasetFacetFields]] = field(factory=list)  # type: ignore[assignment]
    """Nested struct fields."""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacetFields"
        )


@define
class SchemaDatasetFacet(DatasetFacet):
    fields: Optional[List[SchemaDatasetFacetFields]] = field(factory=list)  # type: ignore[assignment]
    """The fields of the data source."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"


utils.register_facet_key("schema", SchemaDatasetFacet)
