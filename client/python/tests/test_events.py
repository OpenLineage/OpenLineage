# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import os
from typing import List, Optional

import attr
import pytest
from openlineage.client import facet, run, set_producer
from openlineage.client.run import RunState
from openlineage.client.serde import Serde


@pytest.fixture(scope='session', autouse=True)
def setup_producer():
    set_producer('https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python')


def get_sorted_json(file_name: str) -> str:
    dirpath = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dirpath, file_name), 'r') as f:
        loaded = json.load(f)
        return json.dumps(loaded, sort_keys=True)


def test_full_core_event_serializes_properly():
    runEvent = run.RunEvent(
        eventType=run.RunState.START,
        eventTime='2021-11-03T10:53:52.427343',
        run=run.Run(
            runId='69f4acab-b87d-4fc0-b27b-8ea950370ff3',
            facets={
                "nominalTime": facet.NominalTimeRunFacet(
                    nominalStartTime='2020-01-01',
                    nominalEndTime='2020-01-02'
                )
            }
        ),
        job=run.Job(
            namespace="openlineage",
            name="name",
            facets={}
        ),
        inputs=[],
        outputs=[],
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python"
    )

    assert Serde.to_json(runEvent) == get_sorted_json('serde_example.json')


def test_run_id_uuid_check():
    # does not throw when passed uuid
    run.Run(runId='69f4acab-b87d-4fc0-b27b-8ea950370ff3')

    with pytest.raises(ValueError):
        run.Run(
            runId='1500100900',
            facets={}
        )


def test_run_event_type_validated():
    valid_event = run.RunEvent(
        RunState.START,
        "2021-11-03T10:53:52.427343",
        run.Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3", {}),
        run.Job("default", "name"),
        "producer"
    )
    with pytest.raises(ValueError):
        run.RunEvent(
            "asdf",
            valid_event.eventTime,
            valid_event.run,
            valid_event.job,
            valid_event.producer
        )

    with pytest.raises(ValueError):
        run.RunEvent(
            valid_event.eventType,
            "2021-11-03",
            valid_event.run,
            valid_event.job,
            valid_event.producer
        )


def test_nominal_time_facet_does_not_require_end_time():
    assert Serde.to_json(facet.NominalTimeRunFacet(
        nominalStartTime='2020-01-01',
    )) == get_sorted_json("nominal_time_without_end.json")


def test_schema_field_default():
    assert Serde.to_json(facet.SchemaField(name='asdf', type='int4')) == \
           '{"name": "asdf", "type": "int4"}'

    assert Serde.to_json(facet.SchemaField(
        name='asdf',
        type='int4',
        description='primary key')
    ) == '{"description": "primary key", "name": "asdf", "type": "int4"}'


@attr.s
class NestedObject:
    value: Optional[int] = attr.ib(default=None)


@attr.s
class NestingObject:
    nested: List[NestedObject] = attr.ib()
    optional: Optional[int] = attr.ib(default=None)


def test_serde_nested_nulls():
    assert Serde.to_json(NestingObject(
        nested=[
            NestedObject(),
            NestedObject(41)
        ],
        optional=3
    )) == '{"nested": [{"value": 41}], "optional": 3}'

    assert Serde.to_json(NestingObject(
        nested=[
            NestedObject()
        ]
    )) == '{"nested": []}'
