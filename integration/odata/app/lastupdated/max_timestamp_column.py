# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Data-write proxy: MAX() of a change-timestamp column via OData.

Works today without admin. Scans the dataset schema for a column whose name matches one of the
configured glob patterns (case-insensitive), then queries ``MAX(column)`` via the backend. This is
the closest available approximation of "last time data was written" for non-persisted views.
"""

from __future__ import annotations

from fnmatch import fnmatch

from app.datasphere.models import Column, Dataset, DatasetSchema
from app.lastupdated.base import LastUpdated

# EDMX types that can represent a point in time.
_TEMPORAL_TYPES = {
    "Edm.DateTimeOffset",
    "Edm.DateTime",
    "Edm.Date",
}


class MaxTimestampColumnResolver:
    name = "max_timestamp_column"

    def __init__(self, patterns: list[str]) -> None:
        # Lower-cased globs for case-insensitive matching.
        self._patterns = [p.lower() for p in patterns]

    def _candidate_columns(self, schema: DatasetSchema) -> list[Column]:
        matches = [c for c in schema.columns if self._matches(c.name)]
        # Prefer explicitly temporal columns; fall back to name-matched columns of any type.
        temporal = [c for c in matches if c.type in _TEMPORAL_TYPES]
        return temporal or matches

    def _matches(self, name: str) -> bool:
        low = name.lower()
        return any(fnmatch(low, pat) for pat in self._patterns)

    def resolve(self, client, dataset: Dataset, schema: DatasetSchema) -> LastUpdated:
        # MAX(column) is queried against the relational OData entity; analytical-only assets have no
        # such endpoint, so this strategy simply doesn't apply (skip, don't error).
        if not dataset.relational_data_url:
            return LastUpdated(None)
        for column in self._candidate_columns(schema):
            value = client.get_max_timestamp(dataset, column.name)
            if value is not None:
                return LastUpdated(value, source=f"max_timestamp_column:{column.name}")
        return LastUpdated(None)
