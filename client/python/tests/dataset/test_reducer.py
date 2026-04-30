# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from openlineage.client.dataset import DatasetConfig, DatasetReducer
from openlineage.client.dataset.trimmers import DatasetNameTrimmer
from openlineage.client.event_v2 import (
    InputDataset,
    OutputDataset,
)
from openlineage.client.facet_v2 import (
    base_subset_dataset,
    column_lineage_dataset,
    input_statistics_input_dataset,
    output_statistics_output_dataset,
    schema_dataset,
)


class TrailingDataPathTrimmer(DatasetNameTrimmer):
    def trim(self, name: str) -> str:
        if not name or "/" not in name:
            return name

        base, last = name.rsplit("/", 1)
        return base if last == "data" else name


class FailingTrimmer(DatasetNameTrimmer):
    def trim(self, name: str) -> str:
        raise Exception("Intentional failure for testing")


class NameLengtheningTrimmer(DatasetNameTrimmer):
    """A trimmer that makes the name longer on every call — would loop forever without a cap."""

    def trim(self, name: str) -> str:
        return name + "/x"


class AlternatingTrimmer(DatasetNameTrimmer):
    """A trimmer that alternates between two values — would loop forever without cycle detection."""

    def trim(self, name: str) -> str:
        parts = name.rstrip("/").split("/")

        if parts[-1] == "a":
            parts[-1] = "b"
        else:
            parts[-1] = "a"

        return "/".join(parts)


TRAILING_DATA_PATH_TRIMMER = f"{TrailingDataPathTrimmer.__module__}.{TrailingDataPathTrimmer.__name__}"
FAILING_TRIMMER = f"{FailingTrimmer.__module__}.{FailingTrimmer.__name__}"
NAME_LENGTHENING_TRIMMER = f"{NameLengtheningTrimmer.__module__}.{NameLengtheningTrimmer.__name__}"
ALTERNATING_TRIMMER = f"{AlternatingTrimmer.__module__}.{AlternatingTrimmer.__name__}"


def _reducer(**kwargs) -> DatasetReducer:
    return DatasetReducer(DatasetConfig(**kwargs))


class TestReduceInputs:
    def test_different_datasets_left_unchanged(self):
        result = _reducer().reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/a"),
                InputDataset(namespace="ns", name="/data/b"),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/a"
        self.assert_does_not_have_input_subset(result[0])
        assert result[1].name == "/data/b"
        self.assert_does_not_have_input_subset(result[1])

    def test_duplicated_trimmed_datasets_are_reduced(self):
        result = _reducer().reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table/day=1"),
                InputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_has_input_subset_locations(result[0], ["/data/table/day=1", "/data/table/day=2"])

    def test_duplicated_datasets_without_trimming_are_reduced(self):
        result = _reducer().reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table"),
                InputDataset(namespace="ns", name="/data/table"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_does_not_have_input_subset(result[0])

    def test_name_is_trimmed_even_when_not_reducing(self):
        result = _reducer().reduce_inputs([InputDataset(namespace="ns", name="/data/table/day=1")])

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_has_input_subset_locations(result[0], ["/data/table/day=1"])

    def test_datasets_with_different_facets_not_reduced(self):
        schema_a = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name="col_a", type="STRING")],
        )
        schema_b = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name="col_b", type="INT")],
        )

        result = _reducer().reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table/day=1", facets={"schema": schema_a}),
                InputDataset(namespace="ns", name="/data/table/day=2", facets={"schema": schema_b}),
                InputDataset(namespace="ns", name="/data/table/day=3", facets={"schema": schema_a}),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/table"
        self.assert_has_input_subset_locations(result[0], ["/data/table/day=1", "/data/table/day=3"])
        assert result[1].name == "/data/table"
        self.assert_has_input_subset_locations(result[1], ["/data/table/day=2"])

    def test_applies_all_active_trimmers(self):
        result = _reducer(
            disabled_trimmers=[
                "openlineage.client.dataset.trimmers.MultiDirDateTrimmer",
                "openlineage.client.dataset.trimmers.YearMonthTrimmer",
            ]
        ).reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table/2024-05-23/key=a"),
                InputDataset(namespace="ns", name="/data/table/2024-06-12/key=b"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_has_input_subset_locations(
            result[0], ["/data/table/2024-05-23/key=a", "/data/table/2024-06-12/key=b"]
        )

    def test_disabling_trimmer_keeps_partitions(self):
        result = _reducer(
            disabled_trimmers=["openlineage.client.dataset.trimmers.KeyValueTrimmer"],
        ).reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table/day=1"),
                InputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/table/day=1"
        assert result[1].name == "/data/table/day=2"

    def test_extra_trimmers_are_applied(self):
        result = _reducer(
            extra_trimmers=[TRAILING_DATA_PATH_TRIMMER],
        ).reduce_inputs([InputDataset(namespace="ns", name="/tmp/table/data")])

        assert len(result) == 1
        assert result[0].name == "/tmp/table"
        self.assert_has_input_subset_locations(result[0], ["/tmp/table/data"])

    def test_preexisting_facets_are_retained(self):
        schema = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name="col_a", type="STRING")],
        )

        result = _reducer().reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table/day=1", facets={"schema": schema}),
                InputDataset(namespace="ns", name="/data/table/day=2", facets={"schema": schema}),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        assert result[0].facets.get("schema") == schema

    def test_preexisting_input_facets_are_retained(self):
        stats = input_statistics_input_dataset.InputStatisticsInputDatasetFacet(rowCount=100, size=2048)

        result = _reducer().reduce_inputs(
            [
                InputDataset(
                    namespace="ns", name="/data/table/day=1", inputFacets={"inputStatistics": stats}
                ),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        assert result[0].inputFacets.get("inputStatistics") == stats

    def test_failing_trimmer_does_not_prevent_reducing(self):
        result = _reducer(
            extra_trimmers=[FAILING_TRIMMER],
        ).reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table/day=1"),
                InputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_has_input_subset_locations(result[0], ["/data/table/day=1", "/data/table/day=2"])

    def test_name_lengthening_trimmer_results_in_no_trimming(self):
        result = _reducer(
            extra_trimmers=[NAME_LENGTHENING_TRIMMER],
        ).reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table/day=1"),
                InputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/table/day=1"
        assert result[1].name == "/data/table/day=2"

    def test_cycle_causing_trimmer_results_in_no_trimming(self):
        result = _reducer(
            extra_trimmers=[ALTERNATING_TRIMMER],
        ).reduce_inputs(
            [
                InputDataset(namespace="ns", name="/data/table/day=1"),
                InputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/table/day=1"
        assert result[1].name == "/data/table/day=2"

    @staticmethod
    def assert_does_not_have_input_subset(input_dataset):
        assert input_dataset.inputFacets.get("subset") is None

    @staticmethod
    def assert_has_input_subset_locations(input_dataset, locations):
        subset = input_dataset.inputFacets.get("subset")
        assert subset is not None
        assert isinstance(subset, base_subset_dataset.InputSubsetInputDatasetFacet)
        assert isinstance(subset.inputCondition, base_subset_dataset.LocationSubsetCondition)
        assert subset.inputCondition.locations == locations


class TestReduceOutputs:
    def test_different_datasets_left_unchanged(self):
        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/a"),
                OutputDataset(namespace="ns", name="/data/b"),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/a"
        self.assert_does_not_have_output_subset(result[0])
        assert result[1].name == "/data/b"
        self.assert_does_not_have_output_subset(result[1])

    def test_duplicated_trimmed_datasets_are_reduced(self):
        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/day=1"),
                OutputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_has_output_subset_locations(
            result[0],
            ["/data/table/day=1", "/data/table/day=2"],
        )

    def test_duplicated_datasets_without_trimming_are_reduced(self):
        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table"),
                OutputDataset(namespace="ns", name="/data/table"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_does_not_have_output_subset(result[0])

    def test_name_is_trimmed_even_when_not_reducing(self):
        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/day=1"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_has_output_subset_locations(
            result[0],
            ["/data/table/day=1"],
        )

    def test_datasets_with_different_facets_not_reduced(self):
        schema_a = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name="col_a", type="STRING")],
        )
        schema_b = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name="col_b", type="INT")],
        )

        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/day=1", facets={"schema": schema_a}),
                OutputDataset(namespace="ns", name="/data/table/day=2", facets={"schema": schema_b}),
                OutputDataset(namespace="ns", name="/data/table/day=3", facets={"schema": schema_a}),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/table"
        self.assert_has_output_subset_locations(
            result[0],
            ["/data/table/day=1", "/data/table/day=3"],
        )
        assert result[1].name == "/data/table"
        self.assert_has_output_subset_locations(
            result[1],
            ["/data/table/day=2"],
        )

    def test_applies_all_active_trimmers(self):
        result = _reducer(
            disabled_trimmers=[
                "openlineage.client.dataset.trimmers.MultiDirDateTrimmer",
                "openlineage.client.dataset.trimmers.YearMonthTrimmer",
            ]
        ).reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/2024-05-23/key=a"),
                OutputDataset(namespace="ns", name="/data/table/2024-06-12/key=b"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_has_output_subset_locations(
            result[0],
            ["/data/table/2024-05-23/key=a", "/data/table/2024-06-12/key=b"],
        )

    def test_disabling_trimmer_keeps_partitions(self):
        result = _reducer(
            disabled_trimmers=["openlineage.client.dataset.trimmers.KeyValueTrimmer"],
        ).reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/day=1"),
                OutputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/table/day=1"
        assert result[1].name == "/data/table/day=2"

    def test_extra_trimmers_are_applied(self):
        result = _reducer(
            extra_trimmers=[TRAILING_DATA_PATH_TRIMMER],
        ).reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/tmp/table/data"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/tmp/table"
        self.assert_has_output_subset_locations(
            result[0],
            ["/tmp/table/data"],
        )

    def test_preexisting_facets_are_retained(self):
        schema = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name="col_a", type="STRING")],
        )

        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/day=1", facets={"schema": schema}),
                OutputDataset(namespace="ns", name="/data/table/day=2", facets={"schema": schema}),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        assert result[0].facets.get("schema") == schema

    def test_preexisting_output_facets_are_retained(self):
        stats = output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(rowCount=200, size=4096)

        result = _reducer().reduce_outputs(
            [
                OutputDataset(
                    namespace="ns", name="/data/table/day=1", outputFacets={"outputStatistics": stats}
                ),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        assert result[0].outputFacets.get("outputStatistics") == stats

    def test_cll_field_level_input_names_are_trimmed(self):
        cll = column_lineage_dataset.ColumnLineageDatasetFacet(
            fields={
                "col_a": column_lineage_dataset.Fields(
                    inputFields=[
                        column_lineage_dataset.InputField(
                            namespace="ns", name="/data/table/day=1", field="col_a"
                        ),
                    ]
                ),
                "col_b": column_lineage_dataset.Fields(
                    inputFields=[
                        column_lineage_dataset.InputField(
                            namespace="ns", name="/data/table/day=2", field="col_b"
                        ),
                    ]
                ),
            },
        )

        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="output", facets={"columnLineage": cll}),
            ]
        )

        assert len(result) == 1
        cll_result = result[0].facets["columnLineage"]
        assert cll_result.fields["col_a"].inputFields[0].name == "/data/table"
        assert cll_result.fields["col_b"].inputFields[0].name == "/data/table"

    def test_cll_dataset_level_input_names_are_trimmed(self):
        cll = column_lineage_dataset.ColumnLineageDatasetFacet(
            fields={},
            dataset=[
                column_lineage_dataset.InputField(namespace="ns", name="/data/table/day=1", field="col_a"),
                column_lineage_dataset.InputField(namespace="ns", name="/data/other/key=x", field="col_b"),
            ],
        )

        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="output", facets={"columnLineage": cll}),
            ]
        )

        assert len(result) == 1
        cll_result = result[0].facets["columnLineage"]
        assert cll_result.dataset[0].name == "/data/table"
        assert cll_result.dataset[1].name == "/data/other"

    def test_cll_names_not_matching_trimmers_left_unchanged(self):
        cll = column_lineage_dataset.ColumnLineageDatasetFacet(
            fields={
                "col_a": column_lineage_dataset.Fields(
                    inputFields=[
                        column_lineage_dataset.InputField(namespace="ns", name="plain_table", field="col_a"),
                    ]
                ),
            },
        )

        result = _reducer().reduce_outputs(
            [
                OutputDataset(namespace="ns", name="output", facets={"columnLineage": cll}),
            ]
        )

        assert result[0].facets["columnLineage"].fields["col_a"].inputFields[0].name == "plain_table"

    def test_failing_trimmer_does_not_prevent_reducing(self):
        result = _reducer(
            extra_trimmers=[FAILING_TRIMMER],
        ).reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/day=1"),
                OutputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 1
        assert result[0].name == "/data/table"
        self.assert_has_output_subset_locations(result[0], ["/data/table/day=1", "/data/table/day=2"])

    def test_name_lengthening_trimmer_results_in_no_trimming(self):
        result = _reducer(
            extra_trimmers=[NAME_LENGTHENING_TRIMMER],
        ).reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/day=1"),
                OutputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/table/day=1"
        assert result[1].name == "/data/table/day=2"

    def test_cycle_causing_trimmer_results_in_no_trimming(self):
        result = _reducer(
            extra_trimmers=[ALTERNATING_TRIMMER],
        ).reduce_outputs(
            [
                OutputDataset(namespace="ns", name="/data/table/day=1"),
                OutputDataset(namespace="ns", name="/data/table/day=2"),
            ]
        )

        assert len(result) == 2
        assert result[0].name == "/data/table/day=1"
        assert result[1].name == "/data/table/day=2"

    @staticmethod
    def assert_does_not_have_output_subset(output_dataset):
        assert output_dataset.outputFacets.get("subset") is None

    @staticmethod
    def assert_has_output_subset_locations(output_dataset, locations):
        subset = output_dataset.outputFacets.get("subset")
        assert subset is not None
        assert isinstance(subset, base_subset_dataset.OutputSubsetOutputDatasetFacet)
        assert isinstance(subset.outputCondition, base_subset_dataset.LocationSubsetCondition)
        assert subset.outputCondition.locations == locations
