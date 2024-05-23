# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define, field
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@define
class Error(RedactMixin):
    errorMessage: str  # noqa: N815
    """Text representation of extraction error message."""

    stackTrace: str | None = field(default=None)  # noqa: N815
    """Stack trace of extraction error message"""

    task: str | None = field(default=None)
    """
    Text representation of task that failed. This can be, for example, SQL statement that parser could
    not interpret.
    """
    taskNumber: int | None = field(default=None)  # noqa: N815
    """Order of task (counted from 0)."""


@define
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
