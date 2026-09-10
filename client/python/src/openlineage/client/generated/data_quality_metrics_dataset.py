# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class ColumnMetrics(RedactMixin):
    nullCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of null values in this column for the rows evaluated"""

    distinctCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of distinct values in this column for the rows evaluated"""

    sum: float | None = attr.field(default=None)  # noqa: A003
    """The total sum of values in this column for the rows evaluated"""

    count: float | None = attr.field(default=None)
    """The number of values in this column"""

    min: float | None = attr.field(default=None)  # noqa: A003
    max: float | None = attr.field(default=None)  # noqa: A003
    quantiles: dict[str, float] | None = attr.field(factory=dict)
    """The property key is the quantile. Examples: 0.1 0.25 0.5 0.75 1"""


@attr.define
class DataQualityMetricsDatasetFacet(DatasetFacet):
    columnMetrics: dict[str, ColumnMetrics]  # noqa: N815
    """The property key is the column name"""

    rowCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of rows evaluated"""

    bytes: int | None = attr.field(default=None)  # noqa: A003
    """The size in bytes"""

    fileCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of files evaluated"""

    lastUpdated: str | None = attr.field(default=None)  # noqa: N815
    """The last time the dataset was changed"""

    captureDate: str | None = attr.field(default=None)  # noqa: N815
    """
    An ISO-8601 timestamp representing the point in time at which these metrics were captured. All
    metrics in this facet (e.g. rowCount, columnMetrics) reflect the dataset state as of this time,
    which lets consumers compare metrics captured at exactly the same moment (e.g. for data-diff).

    Example: 2020-12-17T03:00:00.000Z
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/DataQualityMetricsDatasetFacet.json#/$defs/DataQualityMetricsDatasetFacet"

    @lastUpdated.validator
    def lastupdated_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        if value is None:
            return
        from dateutil import parser

        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)

    @captureDate.validator
    def capturedate_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        if value is None:
            return
        from dateutil import parser

        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)
