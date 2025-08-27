# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import InputDatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class Assertion(RedactMixin):
    assertion: str
    """Type of expectation test that dataset is subjected to"""

    success: bool
    column: str | None = attr.field(default=None)
    """
    Column that expectation is testing. It should match the name provided in SchemaDatasetFacet. If
    column field is empty, then expectation refers to whole dataset.
    """
    _skip_redact: ClassVar[list[str]] = ["column"]


@attr.define
class DataQualityAssertionsDatasetFacet(InputDatasetFacet):
    """list of tests performed on dataset or dataset columns, and their results"""

    assertions: list[Assertion]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json#/$defs/DataQualityAssertionsDatasetFacet"
