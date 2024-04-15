# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define, field
from openlineage.client.generated.base import RunFacet


@define
class ProcessingEngineRunFacet(RunFacet):
    version: str
    """Processing engine version. Might be Airflow or Spark version."""

    name: str | None = field(default=None)
    """Processing engine name, e.g. Airflow or Spark"""

    openlineageAdapterVersion: str | None = field(default=None)  # noqa: N815
    """OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration package version"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet"
