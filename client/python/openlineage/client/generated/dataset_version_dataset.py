# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from attr import define
from openlineage.client import utils
from openlineage.client.generated.base import DatasetFacet


@define
class DatasetVersionDatasetFacet(DatasetFacet):
    datasetVersion: str  # noqa: N815
    """The version of the dataset."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet"


utils.register_facet_key("version", DatasetVersionDatasetFacet)
