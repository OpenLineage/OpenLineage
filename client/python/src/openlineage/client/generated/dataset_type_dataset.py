# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import DatasetFacet


@attr.define
class DatasetTypeDatasetFacet(DatasetFacet):
    datasetType: str  # noqa: N815
    """Dataset type, for example: TABLE|VIEW|FILE|TOPIC|STREAM|MODEL|JOB_OUTPUT."""

    subType: str | None = attr.field(default=None)  # noqa: N815
    """Optional sub-type within the dataset type (e.g., MATERIALIZED, EXTERNAL, TEMPORARY)."""

    _additional_skip_redact: ClassVar[list[str]] = ["datasetType", "subType"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-0/DatasetTypeDatasetFacet.json#/$defs/DatasetTypeDatasetFacet"
