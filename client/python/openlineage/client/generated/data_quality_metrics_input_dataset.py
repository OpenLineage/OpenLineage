# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import InputDatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class ColumnMetrics(RedactMixin):
    nullCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of null values in this column for the rows evaluated"""

    distinctCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of distinct values in this column for the rows evaluated"""

    sum: float | None = attr.field(default=None)
    """The total sum of values in this column for the rows evaluated"""

    count: float | None = attr.field(default=None)
    """The number of values in this column"""

    min: float | None = attr.field(default=None)
    max: float | None = attr.field(default=None)
    quantiles: dict[str, float] | None = attr.field(factory=dict)
    """The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1"""


@attr.define
class DataQualityMetricsInputDatasetFacet(InputDatasetFacet):
    columnMetrics: dict[str, ColumnMetrics]  # noqa: N815
    """The property key is the column name"""

    rowCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of rows evaluated"""

    bytes: int | None = attr.field(default=None)
    """The size in bytes"""

    fileCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of files evaluated"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsInputDatasetFacet.json#/$defs/DataQualityMetricsInputDatasetFacet"
