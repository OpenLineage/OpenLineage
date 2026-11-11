# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""LastUpdated resolver interface."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from app.datasphere.models import Dataset, DatasetSchema


@dataclass(frozen=True)
class LastUpdated:
    value: datetime | None
    # Where the value came from, e.g. "max_timestamp_column:LastChangedAt" or
    # "design_time:deployment_date". Emitted alongside the timestamp for transparency.
    source: str | None = None

    @property
    def found(self) -> bool:
        return self.value is not None


class LastUpdatedResolver(Protocol):
    name: str

    def resolve(self, client, dataset: Dataset, schema: DatasetSchema) -> LastUpdated:
        """Return the last-write timestamp, or ``LastUpdated(None)`` if this strategy can't provide one.

        May raise :class:`app.datasphere.errors.ODataRequestError`, which the caller captures.
        """
        ...
