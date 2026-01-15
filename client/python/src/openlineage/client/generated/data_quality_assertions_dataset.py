# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, ClassVar

import attr
from openlineage.client.generated.base import InputDatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class Assertion(RedactMixin):
    assertion: str
    """
    Type of expectation test that dataset is subjected to

    Example: not_null
    """
    success: bool
    column: str | None = attr.field(default=None)
    """
    Column that expectation is testing. It should match the name provided in SchemaDatasetFacet. If
    column field is empty, then expectation refers to whole dataset.

    Example: id
    """
    severity: str | None = attr.field(default=None)
    """
    The configured severity level of the assertion. Common values are 'error' (test failure blocks
    pipeline) or 'warn' (test failure produces warning only).

    Example: error
    """
    failures: int | None = attr.field(default=None)
    """The actual number of rows that failed the assertion test."""

    properties: dict[str, Any] | None = attr.field(factory=dict)
    """Additional tool-specific properties of the assertion."""

    _skip_redact: ClassVar[list[str]] = ["column"]


@attr.define
class DataQualityAssertionsDatasetFacet(InputDatasetFacet):
    """list of tests performed on dataset or dataset columns, and their results"""

    assertions: list[Assertion]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-0/DataQualityAssertionsDatasetFacet.json#/$defs/DataQualityAssertionsDatasetFacet"
