# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""OpenLineage dataset facet for SAP Datasphere OData errors.

``ODataErrorFacet`` records non-2xx OData failures encountered while collecting a dataset's metadata,
so an errored dataset still produces a DatasetEvent that explains what went wrong (rather than being
silently dropped).

This is not a bespoke facet type: it subclasses the standard :class:`DatasetFacet` and simply adds an
``errors`` property. OpenLineage facets are extensible, so it carries the base ``DatasetFacet``
``_schemaURL`` -- there is no custom schema to ship or host.
"""

from __future__ import annotations

import attr
from openlineage.client.facet_v2 import DatasetFacet

from app.datasphere.errors import ODataError


@attr.define
class ODataErrorEntry:
    operation: str
    message: str
    url: str
    httpStatus: int | None = None  # noqa: N815 - OpenLineage facets use camelCase
    correlationId: str | None = None  # noqa: N815


@attr.define
class ODataErrorFacet(DatasetFacet):
    """Standard DatasetFacet carrying an extra ``errors`` property."""

    errors: list[ODataErrorEntry]


def odata_error_facet(errors: list[ODataError]) -> ODataErrorFacet:
    return ODataErrorFacet(
        errors=[
            ODataErrorEntry(
                operation=e.operation,
                message=e.message,
                url=e.url,
                httpStatus=e.http_status,
                correlationId=e.correlation_id,
            )
            for e in errors
        ]
    )
