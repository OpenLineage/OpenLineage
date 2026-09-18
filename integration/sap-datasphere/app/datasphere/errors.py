# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""OData error types.

Non-2xx OData responses must never abort a scan. Low-level request helpers raise
:class:`ODataRequestError`; the task layer converts it into an :class:`ODataError` (tagged with the
operation that failed) which is surfaced on the dataset's OpenLineage event via ``ODataErrorFacet``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ODataError:
    """A captured OData failure, attached to a dataset event for observability."""

    operation: str  # "row_count" | "schema" | "last_updated" | "list_datasets" | ...
    http_status: int | None
    message: str
    url: str
    correlation_id: str | None = None


class ODataRequestError(Exception):
    """Raised on a non-2xx OData response (4xx or 5xx)."""

    def __init__(
        self,
        message: str,
        *,
        url: str,
        http_status: int | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.url = url
        self.http_status = http_status
        self.correlation_id = correlation_id

    def as_error(self, operation: str) -> ODataError:
        return ODataError(
            operation=operation,
            http_status=self.http_status,
            message=self.message,
            url=self.url,
            correlation_id=self.correlation_id,
        )
