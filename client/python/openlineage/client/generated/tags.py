# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet, JobFacet, RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class Tag(RedactMixin):
    key: str
    """Key that identifies the tag"""

    value: str
    """The value of the field"""

    source: str | None = attr.field(default=None)
    """The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc."""


@attr.define
class TagDataset(Tag):
    field: str | None = attr.field(default=None)
    """Identifies the field in a dataset if a tag applies to one"""


@attr.define
class TagsDatasetFacet(DatasetFacet):
    tags: list[TagDataset] | None = attr.field(factory=list)
    """The tags applied to the dataset facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/2-0-3/TagsDatasetFacet.json#/$defs/TagsDatasetFacet"


@attr.define
class TagsJobFacetFields(JobFacet):
    tags: list[Tag] | None = attr.field(factory=list)

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/2-0-3/TagsJobFacet.json#/$defs/TagsJobFacet"


@attr.define
class TagsRunFacetFields(RunFacet):
    tags: list[Tag] | None = attr.field(factory=list)

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/2-0-3/TagsRunFacet.json#/$defs/TagsRunFacet"
