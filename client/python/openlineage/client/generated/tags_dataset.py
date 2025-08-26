# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from attr import field as attr_field
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@define
class TagsDatasetFacet(DatasetFacet):
    tags: list[TagsDatasetFacetFields] | None = attr_field(factory=list)
    """The tags applied to the dataset facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json#/$defs/TagsDatasetFacet"


@define
class TagsDatasetFacetFields(RedactMixin):
    key: str
    """Key that identifies the tag"""

    value: str
    """The value of the field"""

    source: str | None = attr_field(default=None)
    """The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc."""

    field: str | None = attr_field(default=None)
    """Identifies the field in a dataset if a tag applies to one"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json#/$defs/TagsDatasetFacetFields"
