# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Dataset discovery: which OData entity sets to scan, grouped into spaces.

Discovery is the only source-specific step. Everything after it (``$metadata``, ``$count``,
``$orderby``, paging) is plain OData v4 and lives in :class:`app.datasphere.odata_client.OdataClient`.

  * :class:`CatalogDiscovery` -- SAP Datasphere catalog service (``catalog/spaces``,
    ``spaces('<id>')/assets``). The catalog is itself an OData v4 service, so the include list is
    pushed down as ``$filter=name eq 'A' or name eq 'B'``. (``in (...)`` is not used: the tested
    tenant silently ignores it and returns every space.)
  * :class:`ServiceDocumentDiscovery` -- any OData v4 service. Reads the standard service document
    at each configured service root; every entity set becomes a dataset.

Each strategy also names the dataset namespace scheme and catalog ``framework`` it emits, so existing
Datasphere dataset identities (``sap:datasphere://<host>``) stay unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from app.datasphere.models import Dataset, Space

if TYPE_CHECKING:
    from app.datasphere.odata_client import OdataClient


def odata_string(value: str) -> str:
    """Quote ``value`` as an OData string literal (single quotes, embedded quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


class Discovery(Protocol):
    namespace_scheme: str
    framework: str

    def list_spaces(self, client: OdataClient) -> list[Space]: ...
    def list_datasets(self, client: OdataClient, space_id: str) -> list[Dataset]: ...


class CatalogDiscovery:
    namespace_scheme = "sap:datasphere"
    framework = "sap-datasphere"

    def __init__(self, catalog_url: str, include: list[str] | None = None) -> None:
        self._catalog = catalog_url.rstrip("/")
        self._include = list(include or [])

    def list_spaces(self, client: OdataClient) -> list[Space]:
        from app.datasphere.odata_client import parse_catalog_spaces

        params = None
        if self._include:
            params = {"$filter": " or ".join(f"name eq {odata_string(s)}" for s in self._include)}
        return parse_catalog_spaces(client.collect(f"{self._catalog}/spaces", params))

    def list_datasets(self, client: OdataClient, space_id: str) -> list[Dataset]:
        from app.datasphere.odata_client import parse_catalog_assets

        rows = client.collect(f"{self._catalog}/spaces({odata_string(space_id)})/assets")
        return parse_catalog_assets(rows, space_id)


def parse_service_document(value: list[dict], space_id: str, service_root: str) -> list[Dataset]:
    """Entity sets from an OData v4 service document; singletons and function imports are skipped."""
    root = service_root.rstrip("/")
    datasets: list[Dataset] = []
    for row in value:
        name = row.get("name")
        # ``kind`` is optional and defaults to EntitySet (OData JSON Format v4.0, section 5).
        if not name or row.get("kind", "EntitySet") != "EntitySet":
            continue
        datasets.append(
            Dataset(
                space=space_id,
                asset_id=name,
                name=name,
                entity_set=row.get("url") or name,
                relational_metadata_url=f"{root}/$metadata",
                relational_data_url=f"{root}/",
            )
        )
    return datasets


class ServiceDocumentDiscovery:
    namespace_scheme = "odata"
    framework = "odata"

    def __init__(self, base_url: str, services: list[tuple[str, str]]) -> None:
        base = base_url.rstrip("/")
        self._roots = {name: f"{base}/{path.strip('/')}" for name, path in services}

    def list_spaces(self, client: OdataClient) -> list[Space]:
        return [Space(id=name, name=name) for name in self._roots]

    def list_datasets(self, client: OdataClient, space_id: str) -> list[Dataset]:
        root = self._roots[space_id]
        return parse_service_document(client.collect(root), space_id, root)


def build_discovery(settings) -> Discovery:
    ds = settings.datasphere
    if ds.discovery.type == "services":
        return ServiceDocumentDiscovery(ds.base_url, [(s.name, s.path) for s in ds.discovery.services])
    catalog_url = f"{ds.base_url.rstrip('/')}{ds.odata_base_path.rstrip('/')}/catalog"
    return CatalogDiscovery(catalog_url, include=settings.spaces.include)
