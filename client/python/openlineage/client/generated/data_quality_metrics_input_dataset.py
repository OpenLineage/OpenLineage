# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Optional

from attr import define, field
from openlineage.client.generated.base import InputDatasetFacet
from openlineage.client.utils import RedactMixin


@define
class ColumnMetrics(RedactMixin):
    nullCount: Optional[int] = field(default=None)  # noqa: N815
    """The number of null values in this column for the rows evaluated"""

    distinctCount: Optional[int] = field(default=None)  # noqa: N815
    """The number of distinct values in this column for the rows evaluated"""

    sum: Optional[float] = field(default=None)
    """The total sum of values in this column for the rows evaluated"""

    count: Optional[float] = field(default=None)
    """The number of values in this column"""

    min: Optional[float] = field(default=None)
    max: Optional[float] = field(default=None)
    quantiles: Optional[dict[str, float]] = field(factory=dict)  # type: ignore[assignment]
    """The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1"""


@define
class DataQualityMetricsInputDatasetFacet(InputDatasetFacet):
    columnMetrics: dict[str, ColumnMetrics]  # noqa: N815
    """The property key is the column name"""

    rowCount: Optional[int] = field(default=None)  # noqa: N815
    """The number of rows evaluated"""

    bytes: Optional[int] = field(default=None)
    """The size in bytes"""

    fileCount: Optional[int] = field(default=None)  # noqa: N815
    """The number of files evaluated"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsInputDatasetFacet.json#/$defs/DataQualityMetricsInputDatasetFacet"
