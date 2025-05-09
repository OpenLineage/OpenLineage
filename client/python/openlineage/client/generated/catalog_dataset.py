# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet


@attr.define
class CatalogDatasetFacet(DatasetFacet):
    technology: str
    """The storage framework for which the catalog is configured"""

    source: str
    """Source system where the catalog is configured."""

    type: str
    """Type of the catalog."""

    uri: str
    """URI or connection string to the catalog, if applicable."""

    name: str
    """Name of the catalog, as configured in the source system."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/CatalogDatasetFacet.json#/$defs/CatalogDatasetFacet"
