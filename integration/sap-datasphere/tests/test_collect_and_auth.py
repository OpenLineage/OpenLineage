# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone

import httpx

from app.auth.bearer import BearerAuth
from app.auth.cookie import CookieAuth
from app.datasphere.errors import ODataRequestError
from app.datasphere.models import Column, Dataset, DatasetSchema
from app.lastupdated.design_time import DesignTimeResolver
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
