# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import json
import os
from unittest import mock

import pytest
from attr import asdict, define
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
from openlineage.client.serde import Serde


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


def test_with_additional_properties_adds_new_properties():
    base_facet = BaseFacet()
    changed_facet = base_facet.with_additional_properties(new_prop="new_value")

    assert hasattr(changed_facet, "new_prop")
    assert changed_facet.new_prop == "new_value"


def test_with_additional_properties_updates_existing_properties():
    documentation_facet = documentation_job.DocumentationJobFacet(description="desc")
    changed_facet = documentation_facet.with_additional_properties(description="new_value")

    assert changed_facet.description == "new_value"


def test_with_additional_properties_does_not_overwrite_class_level_attributes():
    base_facet = BaseFacet()
    original_attrs = base_facet.__class__.__attrs_attrs__
    base_facet.with_additional_properties(new_prop="new_value")

    assert base_facet.__class__.__attrs_attrs__ == original_attrs


def test_with_additional_properties_works_with_attr_asdict():
    documentation_facet = documentation_job.DocumentationJobFacet(description="desc")
    changed_facet = documentation_facet.with_additional_properties(new_prop="new_value")

    assert asdict(changed_facet) == {
        "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json#/$defs/DocumentationJobFacet",
        "_deleted": None,
        "description": "desc",
        "contentType": None,
        "new_prop": "new_value",
    }


def test_with_additional_properties_isinstance_works():
    documentation_facet = documentation_job.DocumentationJobFacet(description="desc")
    changed_facet = documentation_facet.with_additional_properties(new_prop="new_value")

    assert isinstance(changed_facet, documentation_job.DocumentationJobFacet)
    assert isinstance(changed_facet, BaseFacet)


def test_facet_copy_serialization_base_facet():
    facet = BaseFacet(producer="producer")
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_parent_run_facet():
    facet = parent_run.ParentRunFacet(
        run=parent_run.Run(runId="3bb703d1-09c1-4a42-8da5-35a0b3216072"),
        job=parent_run.Job(namespace="default", name="parent_job_name"),
        root=parent_run.Root(
            run=parent_run.RootRun("3bb703d1-09c1-4a42-8da5-35a0b3216071"),
            job=parent_run.RootJob(namespace="root_job_namespace", name="root_job_name"),
        ),
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_custom_facet_copy_serialization_success():
    @define
    class SomeFacet(BaseFacet):
        version: str

    facet = SomeFacet(version="1")
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_custom_facet_copy_serialization_fails_when_mixing_attr_classes():
    """This will fail as BaseFacet class uses attr.define and SomeFacet attr.s"""
    import attr

    @attr.s
    class SomeFacet(BaseFacet):
        version: str = attr.ib()

    facet = SomeFacet(version="1")
    facet_copy = copy.deepcopy(facet)
    with pytest.raises(AttributeError, match="'SomeFacet' object has no attribute 'version'"):
        Serde.to_json(facet_copy)
