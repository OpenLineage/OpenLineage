# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from openlineage.client.generated.base import JobFacet


@define
class SQLJobFacet(JobFacet):
    query: str

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet"
