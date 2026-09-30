# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""OData v4 client: pluggable discovery plus standard OData reads.

Which datasets exist comes from a :mod:`discovery <app.datasphere.discovery>` strategy (the SAP
Datasphere catalog by default, or plain OData service documents). Reading each dataset is standard
OData v4 and the same for every source:

  * schema:   GET <metadata URL>                  (Accept: application/xml -> EDMX; cached per URL)
  * count:    GET <service root><entitySet>/$count
  * max(col): GET <service root><entitySet>?$select=..&$orderby=.. desc&$top=1
  * paging:   follows @odata.nextLink

For Datasphere, ``entitySet`` is the asset name with ``.`` -> ``_``, and only assets *exposed for
consumption* appear in the catalog. Non-2xx responses, and 2xx responses whose body cannot be
parsed, raise :class:`ODataRequestError`.
"""

from __future__ import annotations

from datetime import datetime
from urllib.parse import quote, urlencode

import httpx

from app.datasphere._http import get_with_retry
from app.datasphere.discovery import CatalogDiscovery, Discovery
from app.datasphere.edmx import parse_edmx
from app.datasphere.errors import ODataRequestError, bounded_body
from app.datasphere.models import Dataset, DatasetSchema, Space
from app.datasphere.timestamps import parse_iso_timestamp

_JSON = {"accept": "application/json"}
_XML = {"accept": "application/xml"}


def _origin(url: str) -> tuple[str, str, int | None]:
    parsed = httpx.URL(url)
    return parsed.scheme, parsed.host, parsed.port


def _invalid_body(resp: httpx.Response, reason: str) -> ODataRequestError:
    return ODataRequestError(
        f"{reason}: {bounded_body(resp.text)}",
        url=str(resp.request.url),
        http_status=resp.status_code,
    )


def _json_object(resp: httpx.Response) -> dict:
    try:
        body = resp.json()
    except ValueError as exc:
        raise _invalid_body(resp, "invalid JSON response") from exc
    if not isinstance(body, dict):
        raise _invalid_body(resp, "expected a JSON object")
    return body


def _value_rows(resp: httpx.Response, body: dict) -> list[dict]:
    rows = body.get("value", [])
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise _invalid_body(resp, "expected 'value' to be a list of objects")
    return rows


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
        discovery: Discovery | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._discovery = discovery or CatalogDiscovery(f"{self._base_url}{odata_base_path.rstrip('/')}/catalog")
        self._origin = _origin(self._base_url)
        # No redirects: httpx drops Authorization on a cross-origin redirect but keeps a Cookie header,
        # and an expired session redirects to a login page. A 3xx surfaces as ODataRequestError instead.
        self._client = httpx.Client(auth=auth, timeout=timeout, follow_redirects=False)
        # $metadata is per service, and a service can hold many entity sets: fetch each document once.
        self._metadata: dict[str, httpx.Response] = {}

    @property
    def discovery(self) -> Discovery:
        return self._discovery

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> OdataClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------ helpers

    def _get(self, url: str, headers: dict, params: dict | None = None) -> httpx.Response:
        # Metadata/data URLs and @odata.nextLink come from the server; credentials must only ever go
        # to the configured base_url origin.
        if _origin(url) != self._origin:
            raise ODataRequestError(f"refusing request outside the base_url origin {self._base_url}", url=url)
        # Encode spaces as %20: the Datasphere catalog service rejects the form-style '+' that httpx
        # would use for ``params`` ("$filter=name eq 'X'" -> HTTP 400). %20 is valid everywhere.
        if params:
            url = f"{url}{'&' if '?' in url else '?'}{urlencode(params, quote_via=quote, safe='$')}"
        return get_with_retry(self._client, url, headers=headers)

    def collect(self, url: str, params: dict | None = None) -> list[dict]:
        """GET an OData collection, following @odata.nextLink pagination."""
        rows: list[dict] = []
        next_url: str | None = url
        first = True
        seen: set[str] = set()
        while next_url:
            resp = self._get(next_url, _JSON, params if first else None)
            body = _json_object(resp)
            rows.extend(_value_rows(resp, body))
            seen.add(str(resp.request.url))
            next_url = body.get("@odata.nextLink")
            if next_url and not next_url.startswith("http"):
                next_url = f"{self._base_url}/{next_url.lstrip('/')}"
            if next_url and str(httpx.URL(next_url)) in seen:
                raise ODataRequestError("pagination cycle: @odata.nextLink repeats a visited page", url=next_url)
            first = False
        return rows

    # ------------------------------------------------------------------ API

    def list_spaces(self) -> list[Space]:
        return self._discovery.list_spaces(self)

    def list_datasets(self, space_id: str) -> list[Dataset]:
        return self._discovery.list_datasets(self, space_id)

    def get_schema(self, dataset: Dataset) -> DatasetSchema:
        # Prefer the relational $metadata; fall back to the analytical one for analytical-only assets
        # (both are EDMX and parse the same way).
        url = dataset.relational_metadata_url or dataset.analytical_metadata_url
        if not url:
            raise ODataRequestError(
                "asset has no metadata URL",
                url=f"{dataset.space}/{dataset.asset_id}",
            )
        resp = self._metadata.get(url)
        if resp is None:
            resp = self._metadata[url] = self._get(url, _XML)
        try:
            return parse_edmx(resp.text, entity_type_name=dataset.entity_set)
        except (SyntaxError, ValueError) as exc:  # ParseError is a SyntaxError; defusedxml raises ValueError
            raise _invalid_body(resp, f"invalid EDMX ({exc})") from exc

    def get_row_count(self, dataset: Dataset) -> int:
        url = self._relational_entity_url(dataset) + "/$count"
        resp = self._get(url, _JSON)
        try:
            return int(resp.text.strip())
        except ValueError as exc:
            raise _invalid_body(resp, "non-integer $count") from exc

    def get_max_timestamp(self, dataset: Dataset, column: str) -> datetime | None:
        url = self._relational_entity_url(dataset)
        resp = self._get(
            url,
            _JSON,
            params={"$select": column, "$orderby": f"{column} desc", "$top": "1"},
        )
        rows = _value_rows(resp, _json_object(resp))
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
