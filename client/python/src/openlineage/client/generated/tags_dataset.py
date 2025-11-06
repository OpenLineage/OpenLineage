# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class TagsDatasetFacet(DatasetFacet):
    tags: list[TagsDatasetFacetFields] | None = attr.field(factory=list)
    """The tags applied to the dataset facet"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json#/$defs/TagsDatasetFacet"


@attr.define
class TagsDatasetFacetFields(RedactMixin):
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
    """The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc."""

    field: str | None = attr.field(default=None)
    """
    Identifies the field in a dataset if a tag applies to one

    Example: email_address
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json#/$defs/TagsDatasetFacetFields"
