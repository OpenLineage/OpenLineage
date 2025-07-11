# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import InputDatasetFacet


@attr.define
class InputStatisticsInputDatasetFacet(InputDatasetFacet):
    rowCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of rows read"""

    size: int | None = attr.field(default=None)
    """The size in bytes read"""

    fileCount: int | None = attr.field(default=None)  # noqa: N815
    """The number of files read"""

    _additional_skip_redact: ClassVar[list[str]] = ["rowCount", "size"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/InputStatisticsInputDatasetFacet.json#/$defs/InputStatisticsInputDatasetFacet"
