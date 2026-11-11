# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone

from app.config import LastUpdatedConfig
from app.datasphere.errors import ODataRequestError
from app.datasphere.models import Column, Dataset, DatasetSchema
from app.lastupdated import build_resolvers
from app.lastupdated.design_time import DesignTimeResolver
from app.lastupdated.max_timestamp_column import MaxTimestampColumnResolver

DS = Dataset(
    space="S",
    asset_id="sap.s.V",
    name="V",
    relational_data_url="https://h/consumption/relational/S/sap.s.V/",
    deployment_date=datetime(2025, 8, 29, tzinfo=timezone.utc),
)
SCHEMA = DatasetSchema(
    columns=[
        Column("Name", "Edm.String"),
        Column("LastChangedAt", "Edm.DateTimeOffset"),
    ]
)


class FakeClient:
    def __init__(self, max_value=None):
        self._max = max_value
        self.calls = []

    def get_max_timestamp(self, dataset, column):
        self.calls.append(column)
        return self._max


def test_design_time_uses_deployment_date():
    res = DesignTimeResolver().resolve(FakeClient(), DS, SCHEMA)
    assert res.found and res.source == "design_time:deployment_date"


def test_max_timestamp_column_picks_temporal_column():
    val = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    client = FakeClient(max_value=val)
    r = MaxTimestampColumnResolver(["*lastchanged*", "*timestamp*"])
    res = r.resolve(client, DS, SCHEMA)
    assert res.found and res.value == val
    assert res.source == "max_timestamp_column:LastChangedAt"
    assert client.calls == ["LastChangedAt"]


def test_max_timestamp_column_none_when_no_match():
    r = MaxTimestampColumnResolver(["*nope*"])
    assert not r.resolve(FakeClient(), DS, SCHEMA).found


def test_build_resolvers_order_and_types():
    cfg = LastUpdatedConfig(
        strategies=["monitoring", "max_timestamp_column", "design_time"],
        timestamp_column_patterns=["*x*"],
    )
    resolvers = build_resolvers(cfg)
    assert [r.name for r in resolvers] == ["monitoring", "max_timestamp_column", "design_time"]


def test_resolver_error_propagates_as_odata_error():
    class Boom:
        def get_max_timestamp(self, dataset, column):
            raise ODataRequestError("nope", url="u", http_status=403)

    r = MaxTimestampColumnResolver(["*lastchanged*"])
    try:
        r.resolve(Boom(), DS, SCHEMA)
        raise AssertionError("expected ODataRequestError")
    except ODataRequestError as e:
        assert e.http_status == 403
