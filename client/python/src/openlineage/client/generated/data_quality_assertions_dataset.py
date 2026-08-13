# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, ClassVar

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@attr.define
class Assertion(RedactMixin):
    assertion: str
    """
    Classification of the test, e.g. 'not_null', 'unique', 'row_count', 'freshness', 'custom_sql'.

    Example: not_null
    """
    success: bool
    """
    Whether the test found issues: 'true' (no issues found), 'false' (issues found). Independent of
    severity - a test can fail without blocking the pipeline when severity is 'warn'.
    """
    column: str | None = attr.field(default=None)
    """
    Column that test refers to. It should match the name provided in SchemaDatasetFacet. If column field
    is empty, then test refers to whole dataset.

    Example: id
    """
    severity: str | None = attr.field(default=None)
    """
    The configured severity level of the assertion. Common values are 'error' (test failure blocks
    pipeline) or 'warn' (test failure produces warning only).

    Example: error
    """
    name: str | None = attr.field(default=None)
    """
    Name identifying the test.

    Example: assert_no_orphans
    """
    description: str | None = attr.field(default=None)
    """
    Human-readable description of what the assertion checks.

    Example: Ensures all order IDs are unique across the table.
    """
    expected: str | None = attr.field(default=None)
    """
    The expected value or threshold for the assertion, serialized as a string.

    Example: 1000
    """
    actual: str | None = attr.field(default=None)
    """
    The actual value observed during the assertion, serialized as a string.

    Example: 999
    """
    content: str | None = attr.field(default=None)
    """
    The assertion body, e.g. a SQL query or expression.

    Example: SELECT COUNT(*) FROM orders WHERE amount < 0
    """
    contentType: str | None = attr.field(default=None)  # noqa: N815
    """
    The format of the content field, allowing consumers to interpret or filter assertion content. Common
    values include 'sql', 'json', 'expression'.

    Example: sql
    """
    params: dict[str, Any] | None = attr.field(factory=dict)
    """Arbitrary key-value pairs for assertion-specific inputs."""

    _skip_redact: ClassVar[list[str]] = ["column"]


@attr.define
class DataQualityAssertionsDatasetFacet(DatasetFacet):
    """list of tests performed on dataset or dataset columns, and their results"""

    assertions: list[Assertion]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-2-0/DataQualityAssertionsDatasetFacet.json#/$defs/DataQualityAssertionsDatasetFacet"
