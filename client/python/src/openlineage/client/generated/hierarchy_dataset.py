# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class HierarchyDatasetFacet(DatasetFacet):
    hierarchy: list[HierarchyDatasetFacetLevel]
    """
    Dataset hierarchy levels (e.g. DATABASE -> SCHEMA -> TABLE), from highest to lowest level. The order
    is important
    """

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-0/HierarchyDatasetFacet.json#/$defs/HierarchyDatasetFacet"
        )


@attr.define
class HierarchyDatasetFacetLevel(RedactMixin):
    type: str  # noqa: A003
    """
    Dataset hierarchy level type

    Example: DATABASE|SCHEMA|TABLE
    """
    name: str
    """
    Dataset hierarchy level name

    Example: my_db|my_schema|my_table
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/HierarchyDatasetFacet.json#/$defs/HierarchyDatasetFacetLevel"
