# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import JobFacet


@attr.define
class DocumentationJobFacet(JobFacet):
    description: str
    """The description of the job."""

    contentType: str | None = attr.field(default=None)  # noqa: N815
    """
    MIME type of the description field content.

    Example: application/json, text/markdown
    """
    key: ClassVar[str] = "documentation"

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json#/$defs/DocumentationJobFacet"
        )
