# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet


@attr.define
class CatalogDatasetFacet(DatasetFacet):
    framework: str
    """
    The storage framework for which the catalog is configured

    Example: iceberg, delta, hive
    """
    type: str  # noqa: A003
    """
    Type of the catalog.

    Example: jdbc, glue, polaris
    """
    name: str
    """
    Name of the catalog, as configured in the source system.

    Example: my_iceberg_catalog
    """
    metadataUri: str | None = attr.field(default=None)  # noqa: N815
    """
    URI or connection string to the catalog, if applicable.

    Example: jdbc:mysql://host:3306/iceberg_database
    """
    warehouseUri: str | None = attr.field(default=None)  # noqa: N815
    """
    URI or connection string to the physical location of the data that the catalog describes.

    Example: s3://bucket/path/to/iceberg/warehouse
    """
    source: str | None = attr.field(default=None)
    """
    Source system where the catalog is configured.

    Example: spark, flink, hive
    """
    catalogProperties: dict[str, str] | None = attr.field(factory=dict)  # noqa: N815
    """Additional catalog properties"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-0/CatalogDatasetFacet.json#/$defs/CatalogDatasetFacet"
