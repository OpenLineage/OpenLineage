# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

from attr import define, field
from openlineage.client.generated.base import OutputDatasetFacet


@define
class OutputStatisticsOutputDatasetFacet(OutputDatasetFacet):
    rowCount: int  # noqa: N815
    """
    The number of rows written to the dataset
    """
    size: int | None = field(default=None)
    """
    The size in bytes written to the dataset
    """
    _additional_skip_redact: ClassVar[list[str]] = ["rowCount", "size"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/OutputStatisticsOutputDatasetFacet.json#/$defs/OutputStatisticsOutputDatasetFacet"
