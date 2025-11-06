# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import RunFacet


@attr.define
class ProcessingEngineRunFacet(RunFacet):
    version: str
    """
    Processing engine version. Might be Airflow or Spark version.

    Example: 2.5.0
    """
    name: str | None = attr.field(default=None)
    """
    Processing engine name, e.g. Airflow or Spark

    Example: Airflow
    """
    openlineageAdapterVersion: str | None = attr.field(default=None)  # noqa: N815
    """
    OpenLineage adapter package version. Might be e.g. OpenLineage Airflow integration package version

    Example: 0.19.0
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet"
