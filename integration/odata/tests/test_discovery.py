# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import textwrap

import httpx
import pytest
from openlineage.client.serde import Serde
from pydantic import ValidationError

from app.config import load_settings
from app.datasphere.discovery import (
    CatalogDiscovery,
    ServiceDocumentDiscovery,
    build_discovery,
    odata_string,
    parse_service_document,
)
from app.datasphere.odata_client import OdataClient
from app.lineage.mapping import DatasetMetadata, build_dataset_event

BASE = "https://h"

SERVICE_DOC = {
    "@odata.context": "$metadata",
    "value": [
        {"name": "Customers", "kind": "EntitySet", "url": "Customers"},
        {"name": "Orders", "url": "Orders"},  # kind omitted -> EntitySet
        {"name": "Me", "kind": "Singleton", "url": "Me"},
        {"name": "TopSellers", "kind": "FunctionImport", "url": "TopSellers"},
    ],
}

METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="Shop">
      <EntityType Name="Customer"><Key><PropertyRef Name="ID"/></Key>
        <Property Name="ID" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityType Name="Order"><Key><PropertyRef Name="OrderID"/></Key>
        <Property Name="OrderID" Type="Edm.Int32" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Customers" EntityType="Shop.Customer"/>
        <EntitySet Name="Orders" EntityType="Shop.Order"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _client(handler, discovery):
    client = OdataClient(base_url=BASE, auth=httpx.Auth(), discovery=discovery)
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    return client


def test_odata_string_escapes_quotes():
    assert odata_string("O'Brien") == "'O''Brien'"


def test_catalog_pushes_include_filter_with_percent20():
    seen = []

    def handler(request):
        seen.append(request.url.raw_path.decode())
        return httpx.Response(200, json={"value": [{"name": "A"}, {"name": "B"}]})

    discovery = CatalogDiscovery(f"{BASE}/odata/catalog", include=["A", "B"])
    assert [s.id for s in _client(handler, discovery).list_spaces()] == ["A", "B"]
    assert seen == ["/odata/catalog/spaces?$filter=name%20eq%20%27A%27%20or%20name%20eq%20%27B%27"]


def test_catalog_without_include_lists_all_and_quotes_space_key():
    seen = []

    def handler(request):
        seen.append(request.url.raw_path.decode())
        return httpx.Response(200, json={"value": []})

    client = _client(handler, CatalogDiscovery(f"{BASE}/odata/catalog"))
    client.list_spaces()
    client.list_datasets("O'X")
    assert seen[0] == "/odata/catalog/spaces"
    assert seen[1] == "/odata/catalog/spaces('O''X')/assets"


def test_parse_service_document_keeps_only_entity_sets():
    datasets = parse_service_document(SERVICE_DOC["value"], "SHOP", f"{BASE}/svc/")
    assert [d.asset_id for d in datasets] == ["Customers", "Orders"]
    assert datasets[0].entity_set == "Customers"
    assert datasets[0].relational_metadata_url == f"{BASE}/svc/$metadata"
    assert datasets[0].relational_data_url == f"{BASE}/svc/"


def test_services_discovery_reads_service_document_and_fetches_metadata_once():
    calls = []

    def handler(request):
        path = request.url.path
        calls.append(path)
        if path == "/svc":
            return httpx.Response(200, json=SERVICE_DOC)
        if path == "/svc/$metadata":
            return httpx.Response(200, text=METADATA)
        if path.endswith("/$count"):
            return httpx.Response(200, text="7")
        return httpx.Response(404)

    client = _client(handler, ServiceDocumentDiscovery(BASE, [("SHOP", "/svc")]))
    assert [s.id for s in client.list_spaces()] == ["SHOP"]
    datasets = client.list_datasets("SHOP")
    schemas = {d.asset_id: client.get_schema(d) for d in datasets}

    # Entity set -> entity type resolved through EntityContainer (names differ: Orders -> Order).
    assert [c.name for c in schemas["Orders"].columns] == ["OrderID", "ModifiedAt"]
    assert [c.name for c in schemas["Customers"].columns] == ["ID"]
    assert calls.count("/svc/$metadata") == 1
    assert client.get_row_count(datasets[1]) == 7
    assert calls[-1] == "/svc/Orders/$count"


def test_services_mode_emits_odata_namespace_and_framework():
    [ds] = parse_service_document([{"name": "Orders"}], "SHOP", f"{BASE}/svc")
    discovery = ServiceDocumentDiscovery(BASE, [("SHOP", "/svc")])
    ev = build_dataset_event(
        DatasetMetadata(dataset=ds),
        tenant_host="h",
        producer="p",
        namespace_scheme=discovery.namespace_scheme,
        framework=discovery.framework,
    )
    assert ev.dataset.namespace == "odata://h"
    assert ev.dataset.name == "SHOP.Orders"
    assert Serde.to_dict(ev)["dataset"]["facets"]["catalog"]["framework"] == "odata"


def _settings(tmp_path, discovery_yaml):
    p = tmp_path / "config.yaml"
    body = textwrap.dedent(
        """
        datasphere:
          base_url: https://tenant.example
          tenant_host: tenant.example
        """
    ) + textwrap.indent(textwrap.dedent(discovery_yaml), "  ")
    p.write_text(body)
    return load_settings(str(p))


def test_build_discovery_defaults_to_catalog(tmp_path):
    settings = _settings(tmp_path, "")
    assert isinstance(build_discovery(settings), CatalogDiscovery)


def test_services_config(tmp_path):
    settings = _settings(
        tmp_path,
        """
        discovery:
          type: services
          services:
            - {name: SHOP, path: /odata/v4/shop}
        """,
    )
    assert isinstance(build_discovery(settings), ServiceDocumentDiscovery)


@pytest.mark.parametrize(
    "discovery_yaml, match",
    [
        ("discovery:\n  type: services\n", "at least one"),
        ("discovery:\n  type: services\n  services:\n    - {name: A, path: https://evil.example/svc}\n", "start with"),
        ("discovery:\n  type: services\n  services:\n    - {name: A, path: /a}\n    - {name: A, path: /b}\n", "unique"),
        ("discovery:\n  type: registry\n", "catalog"),
    ],
)
def test_invalid_services_config(tmp_path, discovery_yaml, match):
    with pytest.raises(ValidationError, match=match):
        _settings(tmp_path, discovery_yaml)
