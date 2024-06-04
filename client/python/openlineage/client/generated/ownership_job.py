# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Optional

from attr import define, field
from openlineage.client.generated.base import JobFacet
from openlineage.client.utils import RedactMixin


@define
class Owner(RedactMixin):
    name: str
    """
    the identifier of the owner of the Job. It is recommended to define this as a URN. For example
    application:foo, user:jdoe, team:data
    """
    type: Optional[str] = field(default=None)
    """The type of ownership (optional)"""


@define
class OwnershipJobFacet(JobFacet):
    owners: Optional[list[Owner]] = field(factory=list)  # type: ignore[assignment]
    """The owners of the job."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json#/$defs/OwnershipJobFacet"
