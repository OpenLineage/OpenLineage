# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import OutputDatasetFacet

from . import BaseSubsetCondition


@attr.define
class OutputSubsetOutputDatasetFacet(OutputDatasetFacet):
    condition: BaseSubsetCondition.ClassToBeSkipped | None = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/OutputSubsetOutputDatasetFacet.json#/$defs/OutputSubsetOutputDatasetFacet"
