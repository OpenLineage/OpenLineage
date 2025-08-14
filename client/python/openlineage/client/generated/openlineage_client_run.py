# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import RunFacet


@attr.define
class OpenlineageClientRunFacet(RunFacet):
    version: str
    """OpenLineage client package version e.g. python or java client."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/OpenlineageClientRunFacet.json#/$defs/OpenlineageClientRunFacet"
