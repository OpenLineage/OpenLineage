# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class TagsRunFacet(RunFacet):
    tags: list[TagsRunFacetFields] | None = attr.field(factory=list)
    """The tags applied to the run facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json#/$defs/TagsRunFacet"


@attr.define
class TagsRunFacetFields(RedactMixin):
    key: str
    """
    Key that identifies the tag

    Example: pii
    """
    value: str
    """
    The value of the field

    Example: true|@user1|production
    """
    source: str | None = attr.field(default=None)
    """
    The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.

    Example: SPARK
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json#/$defs/TagsRunFacetFields"
