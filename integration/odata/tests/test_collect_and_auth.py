# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone

import httpx

from app.auth.bearer import BearerAuth
from app.auth.cookie import CookieAuth
from app.datasphere.errors import ODataRequestError
from app.datasphere.models import Column, Dataset, DatasetSchema
from app.lastupdated.design_time import DesignTimeResolver
from app.lineage.mapping import DatasetMetadata
from app.tasks.odata_to_ol import collect_metadata

DS = Dataset(
    space="S",
    asset_id="sap.s.V",
    name="V",
    relational_data_url="https://h/consumption/relational/S/sap.s.V/",
    deployment_date=datetime(2025, 8, 29, tzinfo=timezone.utc),
)


class PartlyFailingClient:
    """schema ok, row_count 500, max_timestamp unused (design_time wins)."""

    def get_schema(self, dataset):
        return DatasetSchema(columns=[Column("A", "Edm.String")])

    def get_row_count(self, dataset):
        raise ODataRequestError("Internal Server Error", url="u/$count", http_status=500)

    def get_max_timestamp(self, dataset, column):
        raise AssertionError("should not be called; design_time resolves first")


def test_collect_metadata_captures_errors_and_still_resolves():
    meta = collect_metadata(PartlyFailingClient(), [DesignTimeResolver()], DS)
    assert meta.schema is not None and meta.row_count is None
    assert meta.last_updated is not None and meta.last_updated.source == "design_time:deployment_date"
    assert len(meta.errors) == 1
    assert meta.errors[0].operation == "row_count" and meta.errors[0].http_status == 500


ANALYTICAL_ONLY = Dataset(
    space="S",
    asset_id="sap.s.RL_View",
    name="Aging Grid",
    analytical_metadata_url="https://h/consumption/analytical/S/sap.s.RL_View/$metadata",
    analytical_data_url="https://h/consumption/analytical/S/sap.s.RL_View/",
    supports_analytical=True,
)


class AnalyticalOnlyClient:
    """schema resolves (from analytical endpoint); row_count / max_timestamp must NOT be called."""

    def get_schema(self, dataset):
        return DatasetSchema(columns=[Column("LastChangedAt", "Edm.DateTimeOffset")])

    def get_row_count(self, dataset):
        raise AssertionError("row_count must be skipped for analytical-only assets")

    def get_max_timestamp(self, dataset, column):
        raise AssertionError("max_timestamp must be skipped for analytical-only assets")


def test_analytical_only_asset_skips_relational_ops_without_error():
    from app.lastupdated.max_timestamp_column import MaxTimestampColumnResolver

    resolvers = [MaxTimestampColumnResolver(["*lastchanged*"])]
    meta = collect_metadata(AnalyticalOnlyClient(), resolvers, ANALYTICAL_ONLY)
    assert meta.schema is not None and len(meta.schema.columns) == 1  # schema still captured
    assert meta.row_count is None
    assert meta.last_updated is None
    assert meta.errors == []  # analytical-only is expected, not an error


def _header_for(auth, name):
    req = httpx.Request("GET", "https://example.test/x")
    flow = auth.auth_flow(req)
    prepared = next(flow)
    return prepared.headers.get(name)


def test_cookie_auth_sets_cookie_header():
    assert _header_for(CookieAuth("JSESSIONID=abc"), "Cookie") == "JSESSIONID=abc"


def test_bearer_auth_sets_authorization_header():
    assert _header_for(BearerAuth("tok"), "Authorization") == "Bearer tok"


def test_auth_refuses_non_https_urls():
    import pytest

    from app.auth import _HttpsBasicAuth
    from app.auth.oauth2_client_credentials import OAuth2ClientCredentialsAuth

    for auth in (CookieAuth("JSESSIONID=abc"), BearerAuth("tok"), _HttpsBasicAuth("u", "p")):
        flow = auth.auth_flow(httpx.Request("GET", "http://example.test/x"))
        with pytest.raises(ValueError, match="non-HTTPS"):
            next(flow)

    with pytest.raises(ValueError, match="non-HTTPS"):
        OAuth2ClientCredentialsAuth(token_url="http://idp.test/token", client_id="c", client_secret="s")


def test_emit_failure_for_one_dataset_does_not_abort_scan(monkeypatch):
    from app.datasphere.models import Space
    from app.tasks import odata_to_ol

    datasets = [Dataset(space="S", asset_id=n, name=n) for n in ("A", "B", "C")]

    class Client:
        discovery = None

        def list_spaces(self):
            return [Space("S")]

        def list_datasets(self, space_id):
            return datasets

        def close(self):
            pass

    class Emitter:
        emitted = []

        def emit(self, meta, event_time=None):
            if meta.dataset.asset_id == "B":
                raise RuntimeError("HTTP 503 from OpenLineage backend")
            self.emitted.append(meta.dataset.asset_id)

        def close(self):
            pass

    emitter = Emitter()
    monkeypatch.setattr(odata_to_ol, "build_client", lambda settings, secrets: Client())
    monkeypatch.setattr(odata_to_ol, "build_emitter", lambda settings, secrets, discovery=None: emitter)
    monkeypatch.setattr(odata_to_ol, "build_resolvers", lambda cfg: [])
    monkeypatch.setattr(odata_to_ol, "collect_metadata", lambda c, r, ds, space_label=None: DatasetMetadata(ds))

    class Settings:
        spaces = type("Spaces", (), {"include": [], "exclude": []})()
        lastupdated = None

    summary = odata_to_ol.run_odata_to_ol(Settings(), secrets={})
    assert emitter.emitted == ["A", "C"]
    assert summary["emitted"] == 2 and summary["emit_failed"] == 1 and summary["datasets"] == 3
