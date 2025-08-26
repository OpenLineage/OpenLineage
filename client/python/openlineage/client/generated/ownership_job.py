# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from attr import field as attr_field
from openlineage.client.generated.base import JobFacet
from openlineage.client.utils import RedactMixin


@define
class Owner(RedactMixin):
    name: str
    """
    the identifier of the owner of the Job. It is recommended to define this as a URN. For example
    application:foo, user:jdoe, team:data
    """
    type: str | None = attr_field(default=None)  # noqa: A003
    """The type of ownership (optional)"""


@define
class OwnershipJobFacet(JobFacet):
    owners: list[Owner] | None = attr_field(factory=list)
    """The owners of the job."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json#/$defs/OwnershipJobFacet"
