# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from attr import field as attr_field
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@define
class TagsRunFacet(RunFacet):
    tags: list[TagsRunFacetFields] | None = attr_field(factory=list)
    """The tags applied to the run facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json#/$defs/TagsRunFacet"


@define
class TagsRunFacetFields(RedactMixin):
    key: str
    """Key that identifies the tag"""

    value: str
    """The value of the field"""

    source: str | None = attr_field(default=None)
    """The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json#/$defs/TagsRunFacetFields"
