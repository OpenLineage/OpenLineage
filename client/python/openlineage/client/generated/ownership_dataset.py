# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from typing import List, Optional
from openlineage.client.utils import RedactMixin
from attr import define, field
from openlineage.client import utils
from openlineage.client.generated.base import DatasetFacet


@define
class Owner(RedactMixin):
    name: str
    """
    the identifier of the owner of the Dataset. It is recommended to define this as a URN. For example
    application:foo, user:jdoe, team:data
    """
    type: Optional[str] = field(default=None)  # noqa: A003
    """The type of ownership (optional)"""


@define
class OwnershipDatasetFacet(DatasetFacet):
    owners: Optional[List[Owner]] = field(factory=list)  # type: ignore[assignment]
    """The owners of the dataset."""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json#/$defs/OwnershipDatasetFacet"
        )


utils.register_facet_key("ownership", OwnershipDatasetFacet)
