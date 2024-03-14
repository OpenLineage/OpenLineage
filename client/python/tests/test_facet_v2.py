# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from unittest.mock import MagicMock

from attr import define
from openlineage.client.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import PRODUCER, RunFacet, set_producer


def test_set_producer():
    set_producer("http://test.producer")
    run_facet = RunFacet()
    set_producer(PRODUCER)
    assert run_facet._producer == "http://test.producer"  # noqa: SLF001


def test_custom_facet() -> None:
    session = MagicMock()
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

    event_sent = json.loads(session.post.call_args[0][1])

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
