# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Design-time fallback: view deployment/modification date from the repository.

This is NOT the data-write time -- it is when the view *definition* was last deployed/edited. Emitted
with a clearly-labelled source so consumers know it is design-time, not content-refresh.
"""

from __future__ import annotations

from app.datasphere.models import Dataset, DatasetSchema
from app.lastupdated.base import LastUpdated


class DesignTimeResolver:
    name = "design_time"

    def resolve(self, client, dataset: Dataset, schema: DatasetSchema) -> LastUpdated:
        if dataset.deployment_date is not None:
            return LastUpdated(dataset.deployment_date, source="design_time:deployment_date")
        if dataset.modification_date is not None:
            return LastUpdated(dataset.modification_date, source="design_time:modification_date")
        return LastUpdated(None)
