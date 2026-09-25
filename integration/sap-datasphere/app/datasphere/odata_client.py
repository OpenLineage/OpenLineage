# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Pure-OData Datasphere backend (catalog + consumption).

Relies solely on the OData v4 services under a single base path (default ``/dwaas-core/odata/v4``,
cookie auth; set to ``/api/v1/datasphere/consumption`` for the official OAuth API). No non-OData
internal REST calls are used.

  * spaces:   GET {base}/catalog/spaces
  * assets:   GET {base}/catalog/spaces('<S>')/assets      (each row carries absolute OData URLs)
  * schema:   GET <assetRelationalMetadataUrl>              (Accept: application/xml -> EDMX)
  * count:    GET <assetRelationalDataUrl><entitySet>/$count
  * max(col): GET <assetRelationalDataUrl><entitySet>?$select=..&$orderby=.. desc&$top=1

``entitySet`` = asset name with ``.`` -> ``_``. Only assets *exposed for consumption* appear in the
catalog. Non-2xx responses raise :class:`ODataRequestError`.
"""

from __future__ import annotations

from datetime import datetime

import httpx

from app.datasphere._http import get_with_retry
from app.datasphere.edmx import parse_edmx
from app.datasphere.errors import ODataRequestError
from app.datasphere.models import Dataset, DatasetSchema, Space
from app.datasphere.timestamps import parse_iso_timestamp

_JSON = {"accept": "application/json"}
_XML = {"accept": "application/xml"}


def parse_catalog_spaces(value: list[dict]) -> list[Space]:
    return [Space(id=s["name"], name=s.get("label") or s["name"]) for s in value if s.get("name")]


def parse_catalog_assets(value: list[dict], space_id: str) -> list[Dataset]:
    datasets: list[Dataset] = []
    for a in value:
        name = a.get("name")
        if not name:
            continue
        datasets.append(
            Dataset(
                space=a.get("spaceName") or space_id,
                asset_id=name,
                name=a.get("label") or name,
                relational_metadata_url=a.get("assetRelationalMetadataUrl"),
                relational_data_url=a.get("assetRelationalDataUrl"),
                analytical_metadata_url=a.get("assetAnalyticalMetadataUrl"),
                analytical_data_url=a.get("assetAnalyticalDataUrl"),
                supports_analytical=bool(a.get("supportsAnalyticalQueries")),
                has_parameters=bool(a.get("hasParameters")),
            )
        )
    return datasets


class OdataClient:
    def __init__(
        self,
        base_url: str,
        auth: httpx.Auth,
        odata_base_path: str = "/dwaas-core/odata/v4",
        timeout: int = 180,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._catalog = f"{self._base_url}{odata_base_path.rstrip('/')}/catalog"
        self._client = httpx.Client(auth=auth, timeout=timeout, follow_redirects=True)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> OdataClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------ helpers

    def _get(self, url: str, headers: dict, params: dict | None = None) -> httpx.Response:
        return get_with_retry(self._client, url, headers=headers, params=params)

    def _collect(self, url: str, params: dict | None = None) -> list[dict]:
        """GET an OData collection, following @odata.nextLink pagination."""
        rows: list[dict] = []
        next_url: str | None = url
        first = True
        while next_url:
            resp = self._get(next_url, _JSON, params if first else None)
            body = resp.json()
            rows.extend(body.get("value", []))
            next_url = body.get("@odata.nextLink")
            if next_url and not next_url.startswith("http"):
                next_url = f"{self._base_url}/{next_url.lstrip('/')}"
            first = False
        return rows

    # ------------------------------------------------------------------ API

    def list_spaces(self) -> list[Space]:
        return parse_catalog_spaces(self._collect(f"{self._catalog}/spaces"))

    def list_datasets(self, space_id: str) -> list[Dataset]:
        rows = self._collect(f"{self._catalog}/spaces('{space_id}')/assets")
        return parse_catalog_assets(rows, space_id)

    def get_schema(self, dataset: Dataset) -> DatasetSchema:
        # Prefer the relational $metadata; fall back to the analytical one for analytical-only assets
        # (both are EDMX and parse the same way).
        url = dataset.relational_metadata_url or dataset.analytical_metadata_url
        if not url:
            raise ODataRequestError(
                "asset has no metadata URL",
                url=f"{dataset.space}/{dataset.asset_id}",
            )
        resp = self._get(url, _XML)
        return parse_edmx(resp.text, entity_type_name=dataset.entity_set)

    def get_row_count(self, dataset: Dataset) -> int:
        url = self._relational_entity_url(dataset) + "/$count"
        resp = self._get(url, _JSON)
        return int(resp.text.strip())

    def get_max_timestamp(self, dataset: Dataset, column: str) -> datetime | None:
        url = self._relational_entity_url(dataset)
        resp = self._get(
            url,
            _JSON,
            params={"$select": column, "$orderby": f"{column} desc", "$top": "1"},
        )
        rows = resp.json().get("value", [])
        if not rows:
            return None
        return parse_iso_timestamp(rows[0].get(column))

    def _relational_entity_url(self, dataset: Dataset) -> str:
        if not dataset.relational_data_url:
            raise ODataRequestError(
                "asset has no relational data URL (analytical-only asset)",
                url=f"{dataset.space}/{dataset.asset_id}",
            )
        return f"{dataset.relational_data_url.rstrip('/')}/{dataset.entity_set}"
