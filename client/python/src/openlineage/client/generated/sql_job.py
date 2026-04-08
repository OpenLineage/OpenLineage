# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import JobFacet


@attr.define
class SQLJobFacet(JobFacet):
    query: str
    """Example: SELECT * FROM foo"""

    dialect: str | None = attr.field(default=None)
    """Example: snowflake"""

    key: ClassVar[str] = "sql"

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-0/SQLJobFacet.json#/$defs/SQLJobFacet"
