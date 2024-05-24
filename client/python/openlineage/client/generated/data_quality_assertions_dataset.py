# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from typing import ClassVar, List, Optional
from openlineage.client.utils import RedactMixin
from attr import define, field
from openlineage.client import utils
from openlineage.client.generated.base import InputDatasetFacet


@define
class Assertion(RedactMixin):
    assertion: str
    """Type of expectation test that dataset is subjected to"""

    success: bool
    column: Optional[str] = field(default=None)
    """
    Column that expectation is testing. It should match the name provided in SchemaDatasetFacet. If
    column field is empty, then expectation refers to whole dataset.
    """
    _skip_redact: ClassVar[List[str]] = ["column"]


@define
class DataQualityAssertionsDatasetFacet(InputDatasetFacet):
    """list of tests performed on dataset or dataset columns, and their results"""

    assertions: List[Assertion]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json#/$defs/DataQualityAssertionsDatasetFacet"


utils.register_facet_key("dataQualityAssertions", DataQualityAssertionsDatasetFacet)
