# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define, field
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@define
class Identifier(RedactMixin):
    namespace: str
    """The dataset namespace"""

    name: str
    """The dataset name"""

    type: str
    """Identifier type"""


@define
class SymlinksDatasetFacet(DatasetFacet):
    identifiers: list[Identifier] | None = field(factory=list)  # type: ignore[assignment]

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet"
        )
