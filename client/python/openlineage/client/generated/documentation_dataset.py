# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet


@attr.define
class DocumentationDatasetFacet(DatasetFacet):
    description: str
    """The description of the dataset."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet"
