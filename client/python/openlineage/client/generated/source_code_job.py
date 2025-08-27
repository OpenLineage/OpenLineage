# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import JobFacet


@attr.define
class SourceCodeJobFacet(JobFacet):
    language: str
    """Language in which source code of this job was written."""

    sourceCode: str  # noqa: N815
    """Source code of this job."""

    _additional_skip_redact: ClassVar[list[str]] = ["language"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/SourceCodeJobFacet.json#/$defs/SourceCodeJobFacet"
