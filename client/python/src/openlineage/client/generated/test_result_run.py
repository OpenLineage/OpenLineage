# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum

import attr
from openlineage.client.generated.base import RunFacet


class Severity(Enum):
    """
    The configured severity level of the test. Determines whether a failure blocks pipeline execution
    ('error') or produces a warning only ('warn').
    """

    error = "error"
    warn = "warn"


class Status(Enum):
    """The actual outcome of the test execution."""

    pass_ = "pass"
    fail = "fail"


@attr.define
class TestResultRunFacet(RunFacet):
    """
    Result of a test execution, capturing test outcome and configured severity independently of dataset
    attribution.
    """

    status: Status
    """
    The actual outcome of the test execution.

    Example: pass
    """
    severity: Severity
    """
    The configured severity level of the test. Determines whether a failure blocks pipeline execution
    ('error') or produces a warning only ('warn').

    Example: error
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/TestResultRunFacet.json#/$defs/TestResultRunFacet"
