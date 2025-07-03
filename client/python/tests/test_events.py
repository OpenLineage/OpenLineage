# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import os

import attr
import pytest
from openlineage.client.event_v2 import (
    DatasetEvent,
    InputDataset,
    Job,
    JobEvent,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
    StaticDataset,
)
from openlineage.client.facet_v2 import nominal_time_run, schema_dataset
from openlineage.client.serde import Serde


def get_sorted_json(file_name: str) -> str:
    dirpath = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dirpath, file_name)) as f:
        loaded = json.load(f)
        return json.dumps(loaded, sort_keys=True)


def test_full_core_event_serializes_properly() -> None:
    run_event = RunEvent(
        eventType=RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=Run(
            runId="69f4acab-b87d-4fc0-b27b-8ea950370ff3",
            facets={
                "nominalTime": nominal_time_run.NominalTimeRunFacet(
                    nominalStartTime="2020-01-01T10:53:52.427343",
                    nominalEndTime="2020-01-02T10:53:52.427343",
                ),
            },
        ),
        job=Job(
            namespace="openlineage",
            name="name",
            facets={},
        ),
        inputs=[],
        outputs=[],
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
    )

    assert Serde.to_json(run_event) == get_sorted_json("serde_example_run_event.json")


def test_run_id_uuid_check() -> None:
    # does not throw when passed uuid
    Run(runId="69f4acab-b87d-4fc0-b27b-8ea950370ff3")

    with pytest.raises(ValueError, match="badly formed hexadecimal UUID string"):
        Run(runId="1500100900", facets={})


def test_run_event_type_validated() -> None:
    valid_event = RunEvent(
        eventType=RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3", {}),
        job=Job("default", "name"),
        producer="producer",
    )

    with pytest.raises(ValueError, match="Parsed date-time has to contain time: 2021-11-03"):
        RunEvent(
            eventType=valid_event.eventType,
            eventTime="2021-11-03",
            run=valid_event.run,
            job=valid_event.job,
            producer=valid_event.producer,
        )


def test_nominal_time_facet_does_not_require_end_time() -> None:
    assert Serde.to_json(
        nominal_time_run.NominalTimeRunFacet(
            nominalStartTime="2020-01-01T10:53:52.427343",
        ),
    ) == get_sorted_json("nominal_time_without_end.json")


def test_schema_field_default() -> None:
    assert (
        Serde.to_json(schema_dataset.SchemaDatasetFacetFields(name="asdf", type="int4"))
        == '{"fields": [], "name": "asdf", "type": "int4"}'
    )

    assert (
        Serde.to_json(
            schema_dataset.SchemaDatasetFacetFields(name="asdf", type="int4", description="primary key"),
        )
        == '{"description": "primary key", "fields": [], "name": "asdf", "type": "int4"}'
    )


@attr.define
class NestedObject:
    value: int | None = None


@attr.define
class NestingObject:
    nested: list[NestedObject]
    optional: int | None = None


@attr.define
class ListOfStrings:
    values: list[str]


@attr.define
class NestedListOfStrings:
    nested: list[ListOfStrings]


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
    dataset_event = DatasetEvent(
        eventTime="2021-11-03T10:53:52.427343",
        dataset=StaticDataset(
            namespace="openlineage",
            name="name",
            facets={
                "schema": schema_dataset.SchemaDatasetFacet(
                    fields=[
                        schema_dataset.SchemaDatasetFacetFields(name="a", type="string"),
                        schema_dataset.SchemaDatasetFacetFields(name="b", type="string"),
                    ],
                ),
            },
        ),
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
    )

    assert Serde.to_json(dataset_event) == get_sorted_json("serde_example_dataset_event.json")


def test_job_event() -> None:
    job_event = JobEvent(
        eventTime="2021-11-03T10:53:52.427343",
        job=Job(
            namespace="openlineage",
            name="name",
            facets={},
        ),
        inputs=[
            InputDataset(
                namespace="openlineage",
                name="dataset_a",
                facets={
                    "schema": schema_dataset.SchemaDatasetFacet(
                        fields=[
                            schema_dataset.SchemaDatasetFacetFields(name="a", type="string"),
                        ],
                    ),
                },
            ),
        ],
        outputs=[
            OutputDataset(
                namespace="openlineage",
                name="dataset_b",
                facets={
                    "schema": schema_dataset.SchemaDatasetFacet(
                        fields=[
                            schema_dataset.SchemaDatasetFacetFields(name="a", type="string"),
                        ],
                    ),
                },
            ),
        ],
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python",
    )

    assert Serde.to_json(job_event) == get_sorted_json("serde_example_job_event.json")
