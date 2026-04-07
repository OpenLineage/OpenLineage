# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class TestExecution(RedactMixin):
    """A single test execution and its result."""

    name: str
    """
    Name identifying the test.

    Example: assert_no_orphans
    """
    status: str
    """
    Whether the test found issues: 'pass' (no issues found), 'fail' (issues found), 'skip' (not
    executed). Independent of severity — a test can fail without blocking the pipeline when severity is
    'warn'.

    Example: pass
    """
    severity: str | None = attr.field(default=None)
    """
    The configured consequence of a test failure: 'error' (blocks pipeline execution) or 'warn'
    (produces a warning only, does not block). A test with severity 'warn' and status 'fail' means
    issues were found but execution continued.

    Example: error
    """
    type: str | None = attr.field(default=None)  # noqa: A003
    """
    Classification of the test, e.g. 'not_null', 'unique', 'row_count', 'freshness', 'custom_sql'.

    Example: not_null
    """
    description: str | None = attr.field(default=None)
    """
    Human-readable description of what the test checks.

    Example: Ensures all order IDs are unique across the table.
    """
    expected: str | None = attr.field(default=None)
    """
    The expected value or threshold for the test, serialized as a string.

    Example: 1000
    """
    actual: str | None = attr.field(default=None)
    """
    The actual value observed during the test, serialized as a string.

    Example: 999
    """
    content: str | None = attr.field(default=None)
    """
    The test body, e.g. a SQL query or expression.

    Example: SELECT COUNT(*) FROM orders WHERE amount < 0
    """
    contentType: str | None = attr.field(default=None)  # noqa: N815
    """
    The format of the content field, allowing consumers to interpret or filter test content. Common
    values include 'sql', 'json', 'expression'.

    Example: sql
    """
    params: dict[str, Any] | None = attr.field(factory=dict)
    """Arbitrary key-value pairs for check-specific inputs."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/TestRunFacet.json#/$defs/TestExecution"


@attr.define
class TestRunFacet(RunFacet):
    """
    Results of test executions associated with this run, capturing test outcomes and configured
    severities independently of dataset attribution.
    """

    tests: list[TestExecution]
    """List of test executions and their results."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/TestRunFacet.json#/$defs/TestRunFacet"
