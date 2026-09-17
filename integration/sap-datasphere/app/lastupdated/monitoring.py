# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Real data-write time from the admin/monitoring API.  [SCAFFOLD]

The genuine "last time data was written" for persisted/replicated datasets lives in the Data
Integration Monitor / persistency statistics. That API requires the DW Administrator privilege
(returns HTTP 403 for a standard user / cookie session), so it is not usable on the trial today.

This resolver is wired into the strategy chain but returns no value until implemented against an
admin-scoped session (cookie with monitoring rights, or the OAuth consumption backend). When
implemented, it should query the persistency/monitoring endpoint for the dataset's last load/refresh
timestamp and return it as ``LastUpdated(value, source="monitoring:<endpoint>")``.
"""

from __future__ import annotations

import logging

from app.datasphere.models import Dataset, DatasetSchema
from app.lastupdated.base import LastUpdated

log = logging.getLogger(__name__)


class MonitoringResolver:
    name = "monitoring"

    def resolve(self, client, dataset: Dataset, schema: DatasetSchema) -> LastUpdated:
        # TODO: implement against the admin monitoring/persistency API once access is available:
        #   GET /dwaas-core/monitoring/spaces/<space>/persistency   (403 without admin)
        #   or the equivalent statistics endpoint on the OAuth consumption backend.
        log.debug(
            "monitoring resolver not implemented; skipping",
            extra={"space": dataset.space, "asset": dataset.asset_id},
        )
        return LastUpdated(None)
