# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import os

import attr
import pytest

from openlineage.client import facet, run
from openlineage.client.run import RunState
from openlineage.client.serde import Serde


def get_sorted_json(file_name: str) -> str:
    dirpath = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dirpath, file_name)) as f:
        loaded = json.load(f)
        return json.dumps(loaded, sort_keys=True)


def test_full_core_event_serializes_properly() -> None:
    run_event = run.RunEvent(
        eventType=run.RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=run.Run(
            runId="69f4acab-b87d-4fc0-b27b-8ea950370ff3",
            facets={
                "nominalTime": facet.NominalTimeRunFacet(
                    nominalStartTime="2020-01-01",
                    nominalEndTime="2020-01-02",
                ),
            },
        ),
        job=run.Job(
            namespace="openlineage",
            name="name",
            facets={},
        ),
        inputs=[],
        outputs=[],
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        schemaURL="https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
    )

    assert Serde.to_json(run_event) == get_sorted_json("serde_example_run_event.json")


def test_run_id_uuid_check() -> None:
    # does not throw when passed uuid
    run.Run(runId="69f4acab-b87d-4fc0-b27b-8ea950370ff3")

    with pytest.raises(ValueError, match="badly formed hexadecimal UUID string"):
        run.Run(runId="1500100900", facets={})


def test_run_event_type_validated() -> None:
    valid_event = run.RunEvent(
        RunState.START,
        "2021-11-03T10:53:52.427343",
        run.Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3", {}),
        run.Job("default", "name"),
        "producer",
        "schemaURL",
    )
    with pytest.raises(ValueError, match="'eventType' must be in <enum"):
        run.RunEvent(
            "asdf",
            valid_event.eventTime,
            valid_event.run,
            valid_event.job,
            valid_event.producer,
            valid_event.schemaURL,
        )

    with pytest.raises(ValueError, match="Parsed date-time has to contain time: 2021-11-03"):
        run.RunEvent(
            valid_event.eventType,
            "2021-11-03",
            valid_event.run,
            valid_event.job,
            valid_event.producer,
            valid_event.schemaURL,
        )


def test_nominal_time_facet_does_not_require_end_time() -> None:
    assert Serde.to_json(
        facet.NominalTimeRunFacet(
            nominalStartTime="2020-01-01",
        ),
    ) == get_sorted_json("nominal_time_without_end.json")


def test_schema_field_default() -> None:
    assert (
        Serde.to_json(facet.SchemaField(name="asdf", type="int4"))
        == '{"name": "asdf", "type": "int4"}'
    )

    assert (
        Serde.to_json(
            facet.SchemaField(name="asdf", type="int4", description="primary key"),
        )
        == '{"description": "primary key", "name": "asdf", "type": "int4"}'
    )


@attr.s
class NestedObject:
    value: int | None = attr.ib(default=None)


@attr.s
class NestingObject:
    nested: list[NestedObject] = attr.ib()
    optional: int | None = attr.ib(default=None)


@attr.s
class ListOfStrings:
    values: list[str] = attr.ib()


@attr.s
class NestedListOfStrings:
    nested: list[ListOfStrings] = attr.ib()


def test_serde_list_of_strings() -> None:
    assert (
        Serde.to_json(
            ListOfStrings(
                values=["str_1", "str_2", "str_3"],
            ),
        )
        == '{"values": ["str_1", "str_2", "str_3"]}'
    )

    assert (
        Serde.to_json(
            NestingObject(
                nested=[
                    NestedObject(),
                ],
            ),
        )
        == '{"nested": []}'
    )


def test_serde_nested_list_of_strings() -> None:
    assert (
        Serde.to_json(
            NestedListOfStrings(
                nested=[
                    ListOfStrings(values=["str_1", "str_2", "str_3"]),
                    ListOfStrings(values=["str_a", "str_b", "str_c"]),
                ],
            ),
        )
        == '{"nested": [{"values": ["str_1", "str_2", "str_3"]}, '
        '{"values": ["str_a", "str_b", "str_c"]}]}'
    )

    assert (
        Serde.to_json(
            NestingObject(
                nested=[
                    NestedObject(),
                ],
            ),
        )
        == '{"nested": []}'
    )


def test_serde_nested_nulls() -> None:
    assert (
        Serde.to_json(
            NestingObject(
                nested=[
                    NestedObject(),
                    NestedObject(41),
                ],
                optional=3,
            ),
        )
        == '{"nested": [{"value": 41}], "optional": 3}'
    )

    assert (
        Serde.to_json(
            NestingObject(
                nested=[
                    NestedObject(),
                ],
            ),
        )
        == '{"nested": []}'
    )


def test_dataset_event() -> None:
    dataset_event = run.DatasetEvent(
        eventTime="2021-11-03T10:53:52.427343",
        dataset=run.Dataset(
            namespace="openlineage",
            name="name",
            facets={
                "schema": facet.SchemaDatasetFacet(
                    fields=[
                        facet.SchemaField(name="a", type="string"),
                        facet.SchemaField(name="b", type="string"),
                    ],
                ),
            },
        ),
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        schemaURL="https://openlineage.io/spec/2-0-0/OpenLineage.json#/$defs/DatasetEvent",
    )

    assert Serde.to_json(dataset_event) == get_sorted_json("serde_example_dataset_event.json")


def test_job_event() -> None:
    job_event = run.JobEvent(
        eventTime="2021-11-03T10:53:52.427343",
        job=run.Job(
            namespace="openlineage",
            name="name",
            facets={},
        ),
        inputs=[
            run.Dataset(
                namespace="openlineage",
                name="dataset_a",
                facets={
                    "schema": facet.SchemaDatasetFacet(
                        fields=[
                            facet.SchemaField(name="a", type="string"),
                        ],
                    ),
                },
            ),
        ],
        outputs=[
            run.Dataset(
                namespace="openlineage",
                name="dataset_b",
                facets={
                    "schema": facet.SchemaDatasetFacet(
                        fields=[
                            facet.SchemaField(name="a", type="string"),
                        ],
                    ),
                },
            ),
        ],
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
        schemaURL="https://openlineage.io/spec/2-0-0/OpenLineage.json#/$defs/JobEvent",
    )

    assert Serde.to_json(job_event) == get_sorted_json("serde_example_job_event.json")
