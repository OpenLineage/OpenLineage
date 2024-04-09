# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define, field
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@define
class Owner(RedactMixin):
    name: str
    """
    the identifier of the owner of the Dataset. It is recommended to define this as a URN. For example
    application:foo, user:jdoe, team:data
    """
    type: str | None = field(default=None)
    """The type of ownership (optional)"""


@define
class OwnershipDatasetFacet(DatasetFacet):
    owners: list[Owner] | None = field(factory=list)  # type: ignore[assignment]
    """The owners of the dataset."""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json#/$defs/OwnershipDatasetFacet"
        )
