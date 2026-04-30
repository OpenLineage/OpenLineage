# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import copy
import logging
from typing import Any, Literal

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

BUILTIN_TRIMMERS = (
    KeyValueTrimmer,
    DateTrimmer,
    MultiDirDateTrimmer,
    YearMonthTrimmer,
)


@attr.define
class DatasetConfig:
    reducing_enabled: bool = False
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
    dataset_type: Literal["input", "output"]

    def as_dataset(self) -> Dataset:
        if not self.original_names:
            return self.dataset

        dataset = copy.deepcopy(self.dataset)
        condition = base_subset_dataset.LocationSubsetCondition(locations=sorted(self.original_names))

        if self.dataset_type == "input":
            i_facets = getattr(dataset, "inputFacets", None) or {}
            i_facets["subset"] = base_subset_dataset.InputSubsetInputDatasetFacet(inputCondition=condition)

            base_dict = attr.asdict(dataset, recurse=False)
            base_dict.pop("inputFacets", None)
            return InputDataset(**base_dict, inputFacets=i_facets)
        elif self.dataset_type == "output":
            o_facets = getattr(dataset, "outputFacets", None) or {}
            o_facets["subset"] = base_subset_dataset.OutputSubsetOutputDatasetFacet(outputCondition=condition)

            base_dict = attr.asdict(dataset, recurse=False)
            base_dict.pop("outputFacets", None)
            return OutputDataset(**base_dict, outputFacets=o_facets)
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


class DatasetReducer:
    def __init__(self, config: DatasetConfig) -> None:
        self.config = config
        self.trimmers = config.get_dataset_name_trimmers()

    def reduce_inputs(self, datasets: list[Any]) -> list[Any]:
        return self._reduce_datasets(datasets, dataset_type="input")

    def reduce_outputs(self, datasets: list[Any]) -> list[Any]:
        for ds in datasets:
            self._trim_cll_input_dataset_names(ds)

        return self._reduce_datasets(datasets, dataset_type="output")

    def _reduce_datasets(
        self, datasets: list[Dataset], dataset_type: Literal["input", "output"]
    ) -> list[Dataset]:
        reduced_datasets: list[ReducedDataset] = []

        for ds in datasets:
            original_name = ds.name
            trimmed_name, name_was_trimmed = self._trim(ds.name)
            ds.name = trimmed_name

            reduced = self._find_matching_reduced(ds, reduced_datasets)
            if reduced:
                if name_was_trimmed:
                    reduced.original_names.add(original_name)
            else:
                reduced_datasets.append(
                    ReducedDataset(
                        dataset=ds,
                        original_names={original_name} if name_was_trimmed else set(),
                        dataset_type=dataset_type,
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
                    input_field.name, _ = self._trim(input_field.name)

        for input_field in cll.dataset or ():
            input_field.name, _ = self._trim(input_field.name)

    def _trim(self, name: str) -> tuple[str, bool]:
        max_passes = 16
        original_name = name
        was_trimmed = False
        seen: set[str] = {name}

        for _ in range(max_passes):
            trimmed_name = self._apply_trimmers(name)

            if trimmed_name == name:
                return name, was_trimmed

            if trimmed_name in seen:
                log.warning(
                    "Cycle detected while trimming dataset name '%s': "
                    "name '%s' was repeated after multiple trimmer passes. "
                    "Stopping trimming process and returning original name.",
                    original_name,
                    trimmed_name,
                )
                return original_name, False

            seen.add(trimmed_name)
            name = trimmed_name
            was_trimmed = True

        log.warning(
            "Trimmer loop for dataset name '%s' did not converge after %d passes. "
            "One of the extra trimmers may not be monotonically shortening. "
            "Returning original name.",
            original_name,
            max_passes,
        )
        return original_name, False

    def _apply_trimmers(self, name: str) -> str:
        for trimmer in self.trimmers:
            try:
                name = trimmer.trim(name)
            except Exception as e:
                log.debug(
                    "Skipping trimmer '%s' due to an error when trimming dataset name '%s': %s.",
                    f"{trimmer.__class__.__module__}.{trimmer.__class__.__name__}",
                    name,
                    e,
                )
                continue
        return name

    @staticmethod
    def _find_matching_reduced(ds: Dataset, reduced_datasets: list[ReducedDataset]) -> ReducedDataset | None:
        for reduced in reduced_datasets:
            if reduced.matches(ds):
                return reduced
        return None
