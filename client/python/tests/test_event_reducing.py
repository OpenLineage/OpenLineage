# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent
from openlineage.client.facet_v2 import column_lineage_dataset


def _facets(facets=None):
    return {**(facets or {})}


def _cll_facet(event_id_input_name: str, user_id_input_name: str):
    return column_lineage_dataset.ColumnLineageDatasetFacet(
        fields={
            "event_id": column_lineage_dataset.Fields(
                inputFields=[
                    column_lineage_dataset.InputField(
                        namespace="s3://warehouse", name=event_id_input_name, field="event_id"
                    ),
                ]
            ),
            "user_id": column_lineage_dataset.Fields(
                inputFields=[
                    column_lineage_dataset.InputField(
                        namespace="s3://warehouse", name=user_id_input_name, field="user_id"
                    ),
                ]
            ),
        },
    )


def _event_with_partitioned_datasets() -> RunEvent:
    return RunEvent(
        eventTime="2024-01-15T10:00:00.000Z",
        run=Run(runId="69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
        job=Job(namespace="test-namespace", name="test-job"),
        inputs=[
            InputDataset(namespace="s3://warehouse", name="/data/events/date=2024-01-15", facets=_facets()),
            InputDataset(namespace="s3://warehouse", name="/data/events/date=2024-01-16", facets=_facets()),
        ],
        outputs=[
            OutputDataset(
                namespace="s3://warehouse",
                name="/data/processed_events/date=2024-01-15",
                facets=_facets(
                    {
                        "columnLineage": _cll_facet(
                            event_id_input_name="/data/events/date=2024-01-15",
                            user_id_input_name="/data/events/date=2024-01-16",
                        )
                    }
                ),
            ),
            OutputDataset(
                namespace="s3://warehouse",
                name="/data/processed_events/date=2024-01-16",
                facets=_facets(
                    {
                        "columnLineage": _cll_facet(
                            event_id_input_name="/data/events/date=2024-01-15",
                            user_id_input_name="/data/events/date=2024-01-16",
                        )
                    }
                ),
            ),
        ],
    )


class TestEventReducingEnabled:
    @staticmethod
    def _client(**kwargs):
        return OpenLineageClient(config={"dataset": {"reducing_enabled": True}}, **kwargs)

    def test_inputs_reduced(self, transport):
        self._client(transport=transport).emit(_event_with_partitioned_datasets())
        emitted = transport.event

        assert len(emitted.inputs) == 1
        assert emitted.inputs[0].name == "/data/events"
        assert emitted.inputs[0].namespace == "s3://warehouse"

        subset = emitted.inputs[0].inputFacets["subset"]
        assert subset.inputCondition.locations == [
            "/data/events/date=2024-01-15",
            "/data/events/date=2024-01-16",
        ]

    def test_outputs_reduced(self, transport):
        self._client(transport=transport).emit(_event_with_partitioned_datasets())
        emitted = transport.event

        assert len(emitted.outputs) == 1
        assert emitted.outputs[0].name == "/data/processed_events"
        assert emitted.outputs[0].namespace == "s3://warehouse"

        subset = emitted.outputs[0].outputFacets["subset"]
        assert subset.outputCondition.locations == [
            "/data/processed_events/date=2024-01-15",
            "/data/processed_events/date=2024-01-16",
        ]

    def test_cll_input_names_trimmed(self, transport):
        self._client(transport=transport).emit(_event_with_partitioned_datasets())
        emitted = transport.event

        cll = emitted.outputs[0].facets["columnLineage"]
        assert cll.fields["event_id"].inputFields[0].name == "/data/events"
        assert cll.fields["user_id"].inputFields[0].name == "/data/events"


class TestDefaultClient:
    @staticmethod
    def _client(**kwargs):
        return OpenLineageClient(**kwargs)

    def test_inputs_not_reduced(self, transport):
        self._client(transport=transport).emit(_event_with_partitioned_datasets())
        emitted = transport.event

        assert len(emitted.inputs) == 2
        assert emitted.inputs[0].name == "/data/events/date=2024-01-15"
        assert emitted.inputs[1].name == "/data/events/date=2024-01-16"

    def test_outputs_not_reduced(self, transport):
        self._client(transport=transport).emit(_event_with_partitioned_datasets())
        emitted = transport.event

        assert len(emitted.outputs) == 2
        assert emitted.outputs[0].name == "/data/processed_events/date=2024-01-15"
        assert emitted.outputs[1].name == "/data/processed_events/date=2024-01-16"

    def test_cll_input_names_not_trimmed(self, transport):
        self._client(transport=transport).emit(_event_with_partitioned_datasets())
        emitted = transport.event

        cll = emitted.outputs[0].facets["columnLineage"]
        assert cll.fields["event_id"].inputFields[0].name == "/data/events/date=2024-01-15"
