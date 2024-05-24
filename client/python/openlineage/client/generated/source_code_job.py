# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from attr import define
from openlineage.client import utils
from typing import ClassVar, List
from openlineage.client.generated.base import JobFacet


@define
class SourceCodeJobFacet(JobFacet):
    language: str
    """Language in which source code of this job was written."""

    sourceCode: str  # noqa: N815
    """Source code of this job."""

    _additional_skip_redact: ClassVar[List[str]] = ["language"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/SourceCodeJobFacet.json#/$defs/SourceCodeJobFacet"


utils.register_facet_key("sourceCode", SourceCodeJobFacet)
