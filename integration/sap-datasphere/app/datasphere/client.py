# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""DatasphereClient interface + factory.

There is one client, :class:`app.datasphere.odata_client.OdataClient`, which relies solely on the
OData services (catalog + consumption). It is parameterized by two orthogonal settings: the OData
``base_url`` + ``odata_base_path`` (service root) and the ``auth`` method. The same client therefore
serves both the cookie-authenticated ``/dwaas-core/odata/v4`` services and the official OAuth
``/api/v1/datasphere/consumption`` API -- these are just different (base path, auth) combinations,
not different code paths.

Methods that hit OData raise :class:`app.datasphere.errors.ODataRequestError` on any non-2xx response.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from app.datasphere.models import Dataset, DatasetSchema, Space


@runtime_checkable
class DatasphereClient(Protocol):
    def list_spaces(self) -> list[Space]: ...
    def list_datasets(self, space_id: str) -> list[Dataset]: ...
    def get_schema(self, dataset: Dataset) -> DatasetSchema: ...
    def get_row_count(self, dataset: Dataset) -> int: ...
    def get_max_timestamp(self, dataset: Dataset, column: str) -> datetime | None: ...
    def close(self) -> None: ...


def build_client(settings, secrets) -> DatasphereClient:
    from app.auth import build_auth
    from app.datasphere.odata_client import OdataClient

    return OdataClient(
        base_url=settings.datasphere.base_url,
        auth=build_auth(settings.datasphere.auth, secrets),
        odata_base_path=settings.datasphere.odata_base_path,
        timeout=settings.datasphere.request_timeout_seconds,
    )
