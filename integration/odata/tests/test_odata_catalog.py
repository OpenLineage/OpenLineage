# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from app.datasphere.models import Dataset
from app.datasphere.odata_client import parse_catalog_assets, parse_catalog_spaces

SPACES = [
    {"name": "SAP_S4H", "label": "SAP S/4HANA Content (BDC)"},
    {"name": "USR0569094", "label": "USR0569094"},
    {"label": "no-name-skipped"},
]

ASSETS = [
    {
        "name": "SAP.TIME.VIEW_DIMENSION_YEAR",
        "label": "Time Dimension - Year",
        "spaceName": "SAP_S4H",
        "assetRelationalMetadataUrl": "https://h/api/v1/datasphere/consumption/relational/SAP_S4H/SAP.TIME.VIEW_DIMENSION_YEAR/$metadata",
        "assetRelationalDataUrl": "https://h/api/v1/datasphere/consumption/relational/SAP_S4H/SAP.TIME.VIEW_DIMENSION_YEAR/",
        "assetAnalyticalMetadataUrl": None,
        "assetAnalyticalDataUrl": None,
        "supportsAnalyticalQueries": False,
        "hasParameters": False,
    },
    {"label": "no-name-skipped"},
]


def test_parse_catalog_spaces():
    spaces = parse_catalog_spaces(SPACES)
    assert [s.id for s in spaces] == ["SAP_S4H", "USR0569094"]
    assert spaces[0].name == "SAP S/4HANA Content (BDC)"


def test_parse_catalog_assets_and_entity_set():
    datasets = parse_catalog_assets(ASSETS, "SAP_S4H")
    assert len(datasets) == 1
    d = datasets[0]
    assert d.asset_id == "SAP.TIME.VIEW_DIMENSION_YEAR"
    assert d.entity_set == "SAP_TIME_VIEW_DIMENSION_YEAR"  # dots -> underscores, auto-derived
    assert d.name == "Time Dimension - Year"
    assert d.relational_data_url.endswith("/SAP.TIME.VIEW_DIMENSION_YEAR/")
    assert d.analytical_metadata_url is None and d.analytical_data_url is None
    assert d.supports_analytical is False


def test_entity_set_autoderived_on_manual_dataset():
    d = Dataset(space="S", asset_id="sap.s.My_View", name="x")
    assert d.entity_set == "sap_s_My_View"


def _client(handler):
    import httpx

    from app.datasphere.odata_client import OdataClient

    client = OdataClient(base_url="https://h", auth=httpx.Auth())
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    return client


def test_pagination_cycle_raises():
    import httpx
    import pytest

    from app.datasphere.errors import ODataRequestError

    def handler(request):
        return httpx.Response(200, json={"value": [{"name": "S"}], "@odata.nextLink": str(request.url)})

    with pytest.raises(ODataRequestError, match="pagination cycle"):
        _client(handler).list_spaces()


def test_unparseable_success_bodies_raise_odata_request_error():
    import httpx
    import pytest

    from app.datasphere.errors import ODataRequestError

    ds = Dataset(space="S", asset_id="V", name="V", relational_data_url="https://h/rel/S/V/")
    bad = _client(lambda request: httpx.Response(200, text="<html>login</html>"))

    with pytest.raises(ODataRequestError, match="invalid JSON"):
        bad.list_spaces()
    with pytest.raises(ODataRequestError, match="invalid JSON"):
        bad.get_row_count(ds)
    for count in (None, "x", True, 1.5):
        body = {"value": []} if count is None else {"@odata.count": count, "value": []}
        with pytest.raises(ODataRequestError, match="non-integer @odata.count"):
            _client(lambda request, body=body: httpx.Response(200, json=body)).get_row_count(ds)
    with pytest.raises(ODataRequestError, match="list of objects"):
        _client(lambda request: httpx.Response(200, json={"value": "x"})).list_spaces()


def test_row_count_uses_count_query_option():
    # The official consumption API answers <entitySet>/$count with HTTP 400; $count=true works.
    import httpx

    sent = []

    def handler(request):
        sent.append(request.url)
        return httpx.Response(200, json={"@odata.count": 604, "value": []})

    ds = Dataset(space="S", asset_id="SAP.V", name="V", relational_data_url="https://h/rel/S/SAP.V/")
    assert _client(handler).get_row_count(ds) == 604
    assert sent[0].path == "/rel/S/SAP.V/SAP_V"
    assert dict(sent[0].params) == {"$top": "0", "$count": "true"}

    # Int64 counts may be serialized as strings (IEEE754Compatible=true).
    as_string = _client(lambda request: httpx.Response(200, json={"@odata.count": "604", "value": []}))
    assert as_string.get_row_count(ds) == 604


def test_html_login_page_raises_authentication_error():
    # An unauthenticated or expired session gets the tenant's login page with HTTP 200.
    import httpx
    import pytest

    from app.datasphere.errors import ODataRequestError

    html = {"content-type": "text/html; charset=utf-8"}
    login = _client(lambda request: httpx.Response(200, text="<html>login</html>", headers=html))
    ds = Dataset(space="S", asset_id="V", name="V", relational_metadata_url="https://h/rel/S/V/$metadata")
    with pytest.raises(ODataRequestError, match="authentication required") as exc:
        login.list_spaces()
    assert exc.value.http_status == 200
    with pytest.raises(ODataRequestError, match="authentication required"):
        login.get_schema(ds)


def test_foreign_origin_urls_are_refused_before_sending():
    import httpx
    import pytest

    from app.datasphere.errors import ODataRequestError

    sent = []

    def handler(request):
        sent.append(str(request.url))
        return httpx.Response(200, json={"value": [{"name": "S"}], "@odata.nextLink": "https://evil.example/page2"})

    with pytest.raises(ODataRequestError, match="outside the base_url origin"):
        _client(handler).list_spaces()
    assert all(u.startswith("https://h/") for u in sent)  # the foreign link was never requested

    elsewhere = Dataset(space="S", asset_id="V", name="V", relational_data_url="https://h:8443/rel/S/V/")
    with pytest.raises(ODataRequestError, match="outside the base_url origin"):
        _client(handler).get_row_count(elsewhere)  # same host, different port


def test_redirects_are_not_followed():
    import httpx
    import pytest

    from app.datasphere.errors import ODataRequestError

    def handler(request):
        return httpx.Response(302, headers={"location": "https://login.example/"})

    with pytest.raises(ODataRequestError) as exc:
        _client(handler).list_spaces()
    assert exc.value.http_status == 302


def test_relative_next_link_resolves_against_request_url():
    import httpx

    sent = []

    def handler(request):
        sent.append(str(request.url))
        if "skiptoken" in str(request.url):
            return httpx.Response(200, json={"value": [{"name": "T"}]})
        return httpx.Response(200, json={"value": [{"name": "S"}], "@odata.nextLink": "spaces?$skiptoken=X"})

    spaces = _client(handler).list_spaces()
    assert [s.id for s in spaces] == ["S", "T"]
    assert sent[1] == "https://h/api/v1/datasphere/consumption/catalog/spaces?$skiptoken=X"
