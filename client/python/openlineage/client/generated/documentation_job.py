# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from attr import field as attr_field
from openlineage.client.generated.base import JobFacet


@define
class DocumentationJobFacet(JobFacet):
    description: str
    """The description of the job."""

    contentType: str | None = attr_field(default=None)  # noqa: N815
    """MIME type of the description field content."""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json#/$defs/DocumentationJobFacet"
        )
