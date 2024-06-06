# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet


@attr.define
class StorageDatasetFacet(DatasetFacet):
    storageLayer: str  # noqa: N815
    """Storage layer provider with allowed values: iceberg, delta."""

    fileFormat: str | None = attr.field(default=None)  # noqa: N815
    """File format with allowed values: parquet, orc, avro, json, csv, text, xml."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/StorageDatasetFacet.json#/$defs/StorageDatasetFacet"
