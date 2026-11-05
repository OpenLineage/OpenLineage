# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet


@attr.define
class OneLakeDatasetFacet(DatasetFacet):
    location: str | None = attr.field(default=None)
    """
    Location of the table's files in OneLake, Microsoft Fabric's storage, using IDs only:
    abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{itemId}/Tables/{schema}/{table}, or
    .../Tables/{table} in a lakehouse without schemas. IDs are lowercase, with no trailing slash.

    Example: abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com/05bac213-
    0db7-4b56-ad4c-ac8617938d3f/Tables/sales/orders
    """
    workspaceId: str | None = attr.field(default=None)  # noqa: N815
    """
    ID of the Fabric workspace that holds the item, in lowercase.

    Example: 8b4cb800-a044-43ce-8f76-3356f709b468
    """
    workspaceName: str | None = attr.field(default=None)  # noqa: N815
    """
    Name of the Fabric workspace that holds the item.

    Example: sales-analytics
    """
    itemId: str | None = attr.field(default=None)  # noqa: N815
    """
    ID of the Fabric item that holds the table, such as a lakehouse or a warehouse, in lowercase.

    Example: 05bac213-0db7-4b56-ad4c-ac8617938d3f
    """
    itemName: str | None = attr.field(default=None)  # noqa: N815
    """
    Name of the Fabric item that holds the table.

    Example: lh_sales
    """
    itemType: str | None = attr.field(default=None)  # noqa: N815
    """
    Type of the Fabric item that holds the table, as Fabric names item types. Set only when the producer
    knows it.

    Example: Lakehouse, Warehouse
    """
    tableId: str | None = attr.field(default=None)  # noqa: N815
    """
    ID of the table's Delta metadata (metaData.id in the Delta log). For a table reached through a
    shortcut, this is the ID of the target table.

    Example: 6d3a2bc0-7f1e-4f4b-9a39-2c4e8f1d2b7a
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/OneLakeDatasetFacet.json#/$defs/OneLakeDatasetFacet"
