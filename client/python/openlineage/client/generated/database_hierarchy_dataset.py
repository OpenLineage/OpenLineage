# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class DatabaseHierarchyDatasetFacet(DatasetFacet):
    hierarchy: list[DatabaseHierarchyDatasetFacetLevel]
    """
    Database hierarchy levels (e.g. DATABASE -> SCHEMA -> TABLE), from highest to lowest level. The
    order is important
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/DatabaseHierarchyDatasetFacet.json#/$defs/DatabaseHierarchyDatasetFacet"


@attr.define
class DatabaseHierarchyDatasetFacetLevel(RedactMixin):
    type: str
    """Hierarchy level type"""

    name: str
    """Hierarchy level name"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/DatabaseHierarchyDatasetFacet.json#/$defs/DatabaseHierarchyDatasetFacetLevel"
