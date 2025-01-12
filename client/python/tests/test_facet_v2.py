# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import os
from unittest import mock

from attr import define
from openlineage.client.client import OpenLineageClient
from openlineage.client.event_v2 import (
    BaseEvent,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    PRODUCER,
    BaseFacet,
    RunFacet,
    data_quality_assertions_dataset,
    documentation_job,
    nominal_time_run,
    output_statistics_output_dataset,
    parent_run,
    schema_dataset,
    set_producer,
    tags
)


def test_set_producer():
    set_producer("http://test.producer")
    run_facet = RunFacet()
    set_producer(PRODUCER)
    assert run_facet._producer == "http://test.producer"  # noqa: SLF001


def test_set_producer_from_argument():
    facet = BaseFacet(producer="http://another.producer")
    assert facet._producer == "http://another.producer"  # noqa: SLF001


def test_optional_attributed_not_validated():
    """Don't pass optional value with validator."""
    nominal_time_run.NominalTimeRunFacet(nominalStartTime="2020-12-17T03:00:00.001Z")


def test_custom_facet() -> None:
    session = mock.MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    @define
    class TestRunFacet(RunFacet):
        test_attribute: str

        @staticmethod
        def _get_schema() -> str:
            return "http://test.schema"

    test_run_facet = TestRunFacet(test_attribute="test_attr")

    event = RunEvent(
        eventType=RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3", facets={"test": test_run_facet}),
        job=Job("openlineage", "job"),
        inputs=[],
        outputs=[],
    )

    client.emit(event)

    event_sent = json.loads(session.post.call_args.kwargs["data"])

    expected_event = {
        "eventType": "START",
        "eventTime": "2021-11-03T10:53:52.427343",
        "job": {
            "namespace": "openlineage",
            "name": "job",
            "facets": {},
        },
        "run": {
            "runId": "69f4acab-b87d-4fc0-b27b-8ea950370ff3",
            "facets": {
                "test": {
                    "test_attribute": "test_attr",
                    "_producer": PRODUCER,
                    "_schemaURL": "http://test.schema",
                }
            },
        },
        "inputs": [],
        "outputs": [],
        "producer": PRODUCER,
        "schemaURL": RunEvent._get_schema(),  # noqa: SLF001
    }

    assert expected_event == event_sent


def test_tags_run_facet() -> None:
    session = mock.MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    tags_run_facet = tags.TagsRunFacet(
        tags = [
            tags.Tag(key="test_tag", value="test_value", source="test_source"),
            tags.Tag(key="test_tag2", value="test_value2"),
        ]
    ) 

    event = RunEvent(
        eventType=RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3", facets={"tags": tags_run_facet}), 
        job=Job("openlineage", "job"),
        inputs=[],
        outputs=[],
    )

    client.emit(event)

    event_sent = json.loads(session.post.call_args.kwargs["data"])

    expected_event = {
        "eventType": "START",
        "eventTime": "2021-11-03T10:53:52.427343",
        "job": {
            "namespace": "openlineage",
            "name": "job",
            "facets": {},
        },
        "run": {
            "runId": "69f4acab-b87d-4fc0-b27b-8ea950370ff3",
            "facets": {
                "tags": {
                    "tags": [ 
                        {"key": "test_tag", "value": "test_value", "source": "test_source"},
                        {"key": "test_tag2", "value": "test_value2"},
                    ],
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/TagsRunFacet.json#/$defs/TagsRunFacet",
                }
            },
        },
        "inputs": [],
        "outputs": [],
        "producer": PRODUCER,
        "schemaURL": RunEvent._get_schema(),  # noqa: SLF001
    }
    
    print('expected_event = ', expected_event)
    print('event_sent = ', event_sent)
    assert expected_event == event_sent


def test_tags_job_facet() -> None:
    session = mock.MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    tags_job_facet = tags.TagsJobFacet(
        tags = [
            tags.Tag(key="test_tag", value="test_value", source="test_source"),
            tags.Tag(key="test_tag2", value="test_value2"),
        ]
    ) 

    event = RunEvent(
        eventType=RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3", facets={}), 
        job=Job("openlineage", "job", facets={"tags": tags_job_facet}),
        inputs=[],
        outputs=[],
    )

    client.emit(event)

    event_sent = json.loads(session.post.call_args.kwargs["data"])

    expected_event = {
        "eventType": "START",
        "eventTime": "2021-11-03T10:53:52.427343",
        "job": {
            "namespace": "openlineage",
            "name": "job",
            "facets": {
                "tags": {
                    "tags": [ 
                        {"key": "test_tag", "value": "test_value", "source": "test_source"},
                        {"key": "test_tag2", "value": "test_value2"},
                    ],
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/TagsJobFacet.json#/$defs/TagsJobFacet",
                }
            },
        },
        "run": { "runId": "69f4acab-b87d-4fc0-b27b-8ea950370ff3", "facets": {}},
        "inputs": [],
        "outputs": [],
        "producer": PRODUCER,
        "schemaURL": RunEvent._get_schema(),  # noqa: SLF001
    }
    
    print('expected_event = ', expected_event)
    print('event_sent = ', event_sent)
    assert expected_event == event_sent


def test_tags_dataset_facet() -> None:
    session = mock.MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    tags_dataset_facet = tags.TagsDatasetFacet(
        tags = [
            tags.TagDataset(key="test_tag", value="test_value", field="email", source="test_source"),
            tags.TagDataset(key="test_tag2", value="test_value2"),
        ]
    ) 

    event = RunEvent(
        eventType=RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3", facets={}), 
        job=Job("openlineage", "job", facets={}),
        inputs=[
            InputDataset(
                namespace="some-namespace", 
                name="input-dataset", 
                inputFacets={"tags": tags_dataset_facet} 
            )
        ],
        outputs=[],
    )

    client.emit(event)

    event_sent = json.loads(session.post.call_args.kwargs["data"])

    expected_event = {
        "eventType": "START",
        "eventTime": "2021-11-03T10:53:52.427343",
        "job": {
            "namespace": "openlineage",
            "name": "job",
            "facets": {
                
            },
        },
        "run": { "runId": "69f4acab-b87d-4fc0-b27b-8ea950370ff3", "facets": {}},
        "inputs": [
            {
                "namespace": "some-namespace",
                "name": "input-dataset",
                "facets": {},
                "inputFacets": {
                    "tags": {
                        "tags": [ 
                        {"key": "test_tag", "value": "test_value", "field": "email", "source": "test_source"},
                        {"key": "test_tag2", "value": "test_value2"},
                    ],
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/TagsDatasetFacet.json#/$defs/TagsDatasetFacet",
                    }
                }
            }
        ],
        "outputs": [],
        "producer": PRODUCER,
        "schemaURL": RunEvent._get_schema(),  # noqa: SLF001
    }
    
    print('expected_event = ', expected_event)
    print('event_sent = ', event_sent)
    assert expected_event == event_sent
