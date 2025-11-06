# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import RunFacet


@attr.define
class ExternalQueryRunFacet(RunFacet):
    externalQueryId: str  # noqa: N815
    """
    Identifier for the external system

    Example: my-project-1234:US.bquijob_123x456_123y123z123c
    """
    source: str
    """
    source of the external query

    Example: bigquery
    """

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-2/ExternalQueryRunFacet.json#/$defs/ExternalQueryRunFacet"
        )
