# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import JobFacet
from openlineage.client.utils import RedactMixin


@attr.define
class TagsJobFacet(JobFacet):
    tags: list[TagsJobFacetFields] | None = attr.field(factory=list)
    """The tags applied to the job facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsJobFacet.json#/$defs/TagsJobFacet"


@attr.define
class TagsJobFacetFields(RedactMixin):
    key: str
    """Key that identifies the tag"""

    value: str
    """The value of the field"""

    source: str | None = attr.field(default=None)
    """The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsJobFacet.json#/$defs/TagsJobFacetFields"
