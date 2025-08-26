# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from openlineage.client.generated.base import RunFacet


@define
class ExternalQueryRunFacet(RunFacet):
    externalQueryId: str  # noqa: N815
    """Identifier for the external system"""

    source: str
    """source of the external query"""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-2/ExternalQueryRunFacet.json#/$defs/ExternalQueryRunFacet"
        )
