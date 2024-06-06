# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class Identifier(RedactMixin):
    namespace: str
    """The dataset namespace"""

    name: str
    """The dataset name"""

    type: str
    """Identifier type"""


@attr.define
class SymlinksDatasetFacet(DatasetFacet):
    identifiers: list[Identifier] | None = attr.field(factory=list)

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet"
        )
