# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from attr import field as attr_field
from openlineage.client.generated.base import DatasetFacet


@define
class CatalogDatasetFacet(DatasetFacet):
    framework: str
    """The storage framework for which the catalog is configured"""

    type: str  # noqa: A003
    """Type of the catalog."""

    name: str
    """Name of the catalog, as configured in the source system."""

    metadataUri: str | None = attr_field(default=None)  # noqa: N815
    """URI or connection string to the catalog, if applicable."""

    warehouseUri: str | None = attr_field(default=None)  # noqa: N815
    """URI or connection string to the physical location of the data that the catalog describes."""

    source: str | None = attr_field(default=None)
    """Source system where the catalog is configured."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/CatalogDatasetFacet.json#/$defs/CatalogDatasetFacet"
