# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from attr import field as attr_field
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@define
class Identifier(RedactMixin):
    namespace: str
    """The dataset namespace"""

    name: str
    """The dataset name"""

    type: str  # noqa: A003
    """Identifier type"""


@define
class SymlinksDatasetFacet(DatasetFacet):
    identifiers: list[Identifier] | None = attr_field(factory=list)

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet"
        )
