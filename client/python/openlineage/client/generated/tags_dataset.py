# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class Identifier(RedactMixin):
    key: str
    """Key that identifies the tag"""

    value: str
    """Value of the tag"""

    source: str
    """The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc."""

    field: str
    """Identifies the field in a dataset if a tag applies to one"""


@attr.define
class TagsDatasetFacet(DatasetFacet):
    identifiers: list[Identifier] | None = attr.field(factory=list)

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/2-0-3/TagsDatasetFacet.json#/$defs/TagsDatasetFacet"
