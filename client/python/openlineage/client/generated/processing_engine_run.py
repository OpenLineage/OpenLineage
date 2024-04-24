# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Optional

from attr import define, field
from openlineage.client.generated.base import RunFacet


@define
class ProcessingEngineRunFacet(RunFacet):
    version: str
    """Processing engine version. Might be Airflow or Spark version."""

    name: Optional[str] = field(default=None)
    """Processing engine name, e.g. Airflow or Spark"""

    openlineageAdapterVersion: Optional[str] = field(default=None)  # noqa: N815
    """OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration package version"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet"
