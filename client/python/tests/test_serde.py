# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from typing import Any

import attr
import pytest
from openlineage.client.facet import BaseFacet
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde


@attr.s
class CustomFacet(BaseFacet):
    generated_at: datetime = attr.ib()  # noqa: RUF012
    generated_on: date = attr.ib()
    run_uuid: uuid.UUID = attr.ib()
    unknown: Any = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return "https://example.com/schema.json"


def make_event() -> RunEvent:
    facet = CustomFacet(
        generated_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
        generated_on=date(2026, 9, 10),
        run_uuid=uuid.UUID("0d6512eb-2f76-4e3b-95ab-90692cc8e18f"),
        unknown=object(),
    )
    run = Run(runId="0d6512eb-2f76-4e3b-95ab-90692cc8e18f", facets={"custom": facet})
    return RunEvent(
        eventType=RunState.START,
        eventTime="2026-09-10T12:00:00Z",
        run=run,
        job=Job(namespace="test", name="job"),
        producer="https://example.com/producer",
    )


def test_datetime_facet_field_is_serialized_as_isoformat() -> None:
    wire = json.loads(Serde.to_json(make_event()))
    facet = wire["run"]["facets"]["custom"]
    assert facet["generated_at"] == "2026-09-10T12:00:00+00:00"


def test_date_facet_field_is_serialized_as_isoformat() -> None:
    wire = json.loads(Serde.to_json(make_event()))
    facet = wire["run"]["facets"]["custom"]
    assert facet["generated_on"] == "2026-09-10"


def test_uuid_facet_field_is_serialized_as_string() -> None:
    wire = json.loads(Serde.to_json(make_event()))
    facet = wire["run"]["facets"]["custom"]
    assert facet["run_uuid"] == "0d6512eb-2f76-4e3b-95ab-90692cc8e18f"


def test_unknown_type_still_uses_placeholder() -> None:
    wire = json.loads(Serde.to_json(make_event()))
    facet = wire["run"]["facets"]["custom"]
    assert facet["unknown"] == "<<non-serializable: object>>"


def test_nested_types_inside_lists_and_dicts() -> None:
    obj = {"items": [datetime(2026, 9, 10, 12, 0), {"when": date(2026, 9, 10)}], "skip": None}
    assert Serde.to_dict(obj) == {
        "items": ["2026-09-10T12:00:00", {"when": "2026-09-10"}],
    }


def test_to_dict_converts_well_known_types() -> None:
    obj = {"when": datetime(2026, 9, 10, 12, 0), "id": uuid.UUID("0d6512eb-2f76-4e3b-95ab-90692cc8e18f")}
    assert Serde.to_dict(obj) == {
        "when": "2026-09-10T12:00:00",
        "id": "0d6512eb-2f76-4e3b-95ab-90692cc8e18f",
    }


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 10, 12, 0, 30, 250000),
        date(2026, 9, 10),
    ],
)
def test_roundtrip_through_json(value: datetime | date) -> None:
    wire = json.dumps(Serde.to_dict({"v": value}))
    assert json.loads(wire)["v"] == value.isoformat()
