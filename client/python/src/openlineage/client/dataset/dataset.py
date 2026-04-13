# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import copy
import logging
from typing import Any

import attr
from openlineage.client.dataset.trimmers import (
    DatasetNameTrimmer,
    DateTrimmer,
    KeyValueTrimmer,
    MultiDirDateTrimmer,
    YearMonthTrimmer,
)
from openlineage.client.event_v2 import Dataset, InputDataset, OutputDataset
from openlineage.client.facet_v2 import base_subset_dataset, column_lineage_dataset
from openlineage.client.utils import import_from_string

log = logging.getLogger(__name__)

BUILTIN_TRIMMERS = [
    KeyValueTrimmer,
    DateTrimmer,
    MultiDirDateTrimmer,
    YearMonthTrimmer,
]


@attr.define
class DatasetConfig:
    normalization_enabled: bool = False
    disabled_trimmers: list[str] = attr.field(factory=list)
    extra_trimmers: list[str] = attr.field(factory=list)

    def get_dataset_name_trimmers(self) -> list[DatasetNameTrimmer]:
        active: list[DatasetNameTrimmer] = []
        seen_classes: set[type[DatasetNameTrimmer]] = set()

        for cls in BUILTIN_TRIMMERS:
            fqn = f"{cls.__module__}.{cls.__name__}"
            if fqn in self.disabled_trimmers:
                continue

            if cls not in seen_classes:
                active.append(cls())
                seen_classes.add(cls)

        for fqn in self.extra_trimmers:
            try:
                cls = import_from_string(fqn)
                if not issubclass(cls, DatasetNameTrimmer):
                    log.warning(
                        "Configured trimmer '%s' is not a DatasetNameTrimmer. Skipping.",
                        fqn,
                    )
                    continue

                if cls not in seen_classes:
                    active.append(cls())
                    seen_classes.add(cls)
            except Exception as e:
                log.warning(
                    "Failed to load trimmer '%s': %s. Skipping.",
                    fqn,
                    e,
                )
        return active


@attr.define
class ReducedDataset:
    dataset: Dataset
    original_names: set[str]

    def as_dataset(self) -> Dataset:
        if not self.original_names:
            return self.dataset

        dataset = copy.deepcopy(self.dataset)
        condition = base_subset_dataset.LocationSubsetCondition(locations=sorted(self.original_names))

        if isinstance(dataset, InputDataset):
            i_facets = dataset.inputFacets if dataset.inputFacets is not None else {}
            i_facets["subset"] = base_subset_dataset.InputSubsetInputDatasetFacet(inputCondition=condition)
            dataset.inputFacets = i_facets
        elif isinstance(dataset, OutputDataset):
            o_facets = dataset.outputFacets if dataset.outputFacets is not None else {}
            o_facets["subset"] = base_subset_dataset.OutputSubsetOutputDatasetFacet(outputCondition=condition)
            dataset.outputFacets = o_facets

        return dataset

    def matches(self, other_dataset: Dataset) -> bool:
        return self._name_matches(other_dataset) and self._facets_match(other_dataset)

    def _name_matches(self, other_dataset: Dataset) -> bool:
        return self.dataset.name == other_dataset.name

    def _facets_match(self, other_dataset: Dataset) -> bool:
        if self.dataset.facets != other_dataset.facets:
            return False
        if getattr(self.dataset, "inputFacets", None) != getattr(other_dataset, "inputFacets", None):
            return False
        if getattr(self.dataset, "outputFacets", None) != getattr(other_dataset, "outputFacets", None):
            return False
        return True


class DatasetNormalizer:
    def __init__(self, config: DatasetConfig) -> None:
        self.config = config
        self.trimmers = config.get_dataset_name_trimmers()

    def normalize_inputs(self, datasets: list[Any]) -> list[Any]:
        input_datasets = [
            InputDataset(
                inputFacets={},
                **attr.asdict(ds, recurse=False),
            )
            if not hasattr(ds, "inputFacets")
            else ds
            for ds in datasets
        ]
        return self._reduce_datasets(input_datasets)

    def normalize_outputs(self, datasets: list[Any]) -> list[Any]:
        output_datasets = [
            OutputDataset(
                outputFacets={},
                **attr.asdict(ds, recurse=False),
            )
            if not hasattr(ds, "outputFacets")
            else ds
            for ds in datasets
        ]
        for dataset in output_datasets:
            self._trim_cll_input_dataset_names(dataset)

        return self._reduce_datasets(output_datasets)

    def _reduce_datasets(self, datasets: list[Dataset]) -> list[Dataset]:
        reduced_datasets: list[ReducedDataset] = []

        for ds in datasets:
            original_name = ds.name
            normalized_name, name_was_normalized = self._apply_trimmers(ds.name)
            ds.name = normalized_name

            reduced = self._find_matching_reduced(ds, reduced_datasets)
            if reduced:
                if name_was_normalized:
                    reduced.original_names.add(original_name)
            else:
                reduced_datasets.append(
                    ReducedDataset(
                        dataset=ds,
                        original_names={original_name} if name_was_normalized else set(),
                    )
                )

        return [reduced.as_dataset() for reduced in reduced_datasets]

    def _trim_cll_input_dataset_names(self, dataset: Dataset) -> None:
        if not dataset.facets:
            return

        cll = dataset.facets.get("columnLineage")
        if not isinstance(cll, column_lineage_dataset.ColumnLineageDatasetFacet):
            return

        if cll.fields:
            for field in cll.fields.values():
                for input_field in field.inputFields or ():
                    input_field.name, _ = self._apply_trimmers(input_field.name)

        for input_field in cll.dataset or ():
            input_field.name, _ = self._apply_trimmers(input_field.name)

    def _apply_trimmers(self, name: str) -> tuple[str, bool]:
        was_trimmed = False

        while True:
            new_name = name
            for trimmer in self.trimmers:
                new_name = trimmer.trim(new_name)

            if new_name == name:
                return name, was_trimmed

            name = new_name
            was_trimmed = True

    @staticmethod
    def _find_matching_reduced(ds: Dataset, reduced_datasets: list[ReducedDataset]) -> ReducedDataset | None:
        for reduced in reduced_datasets:
            if reduced.matches(ds):
                return reduced
        return None
