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


#
# def test_full_core_event_serializes_properly(facet_mocker, event_mocker) -> None:
def test_full_core_event_serializes_properly() -> None:
    session = mock.MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    set_producer("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client")

    event_init = BaseEvent.__attrs_post_init__
    facet_init = BaseFacet.__attrs_post_init__

    def set_test_schemaURL(self):  # noqa: N802
        if getattr(self, "schemaURL", None):
            event_init(self)
            self.schemaURL = "http://test.schema.url"
        else:
            facet_init(self)
            self._schemaURL = "http://test.schema.url"

    with mock.patch.object(
        BaseFacet, "__attrs_post_init__", autospec=True, side_effect=set_test_schemaURL
    ), mock.patch.object(BaseEvent, "__attrs_post_init__", autospec=True, side_effect=set_test_schemaURL):
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime="2020-12-28T19:51:01.641Z",
            run=Run(
                runId="ea041791-68bc-4ae1-bd89-4c8106a157e4",
                facets={
                    "nominalTime": nominal_time_run.NominalTimeRunFacet(
                        nominalStartTime="2020-12-17T03:00:00.001Z", nominalEndTime="2020-12-17T04:00:00.001Z"
                    ),
                    "parent": parent_run.ParentRunFacet(
                        run=parent_run.Run(runId="3f5e83fa-3480-44ff-99c5-ff943904e5e8"),
                        job=parent_run.Job(namespace="my-scheduler-namespace", name="myjob.mytask"),
                    ),
                },
            ),
            job=Job(
                namespace="my-scheduler-namespace",
                name="myjob.mytask",
                facets={"documentation": documentation_job.DocumentationJobFacet(description="string")},
            ),
            inputs=[
                InputDataset(
                    namespace="my-datasource-namespace",
                    name="instance.schema.table",
                    inputFacets={
                        "dataQualityAssertions": data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(  # noqa: E501
                            assertions=[
                                data_quality_assertions_dataset.Assertion(
                                    assertion="row_count_equal_to", success=True
                                ),
                                data_quality_assertions_dataset.Assertion(
                                    assertion="no_null_values", success=True, column="id"
                                ),
                            ]
                        )
                    },
                    facets={
                        "schema": schema_dataset.SchemaDatasetFacet(
                            fields=[
                                schema_dataset.SchemaDatasetFacetFields(
                                    name="column1", type="VARCHAR", description="string"
                                )
                            ]
                        )
                    },
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="my-datasource-namespace",
                    name="instance.schema.table",
                    outputFacets={
                        "outputStatistics": output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(  # noqa: E501
                            rowCount=2000, size=2097152
                        )
                    },
                    facets={
                        "schema": schema_dataset.SchemaDatasetFacet(
                            fields=[
                                schema_dataset.SchemaDatasetFacetFields(
                                    name="column1", type="VARCHAR", description="string"
                                )
                            ]
                        )
                    },
                )
            ],
        )

        client.emit(event)

        event_sent = json.loads(session.post.call_args.kwargs["data"])

        dirpath = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dirpath, "example_full_event.json")) as f:
            expected_event = json.load(f)

        assert expected_event == event_sent
