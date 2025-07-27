# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class Error(RedactMixin):
    errorMessage: str  # noqa: N815
    """Text representation of extraction error message."""

    stackTrace: str | None = attr.field(default=None)  # noqa: N815
    """Stack trace of extraction error message"""

    task: str | None = attr.field(default=None)
    """
    Text representation of task that failed. This can be, for example, SQL statement that parser could
    not interpret.
    """
    taskNumber: int | None = attr.field(default=None)  # noqa: N815
    """Order of task (counted from 0)."""


@attr.define
class ExtractionErrorRunFacet(RunFacet):
    totalTasks: int  # noqa: N815
    """
    The number of distinguishable tasks in a run that were processed by OpenLineage, whether
    successfully or not. Those could be, for example, distinct SQL statements.
    """
    failedTasks: int  # noqa: N815
    """
    The number of distinguishable tasks in a run that were processed not successfully by OpenLineage.
    Those could be, for example, distinct SQL statements.
    """
    errors: list[Error]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-2/ExtractionErrorRunFacet.json#/$defs/ExtractionErrorRunFacet"
