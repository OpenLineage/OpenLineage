# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class ExecutionParameter(RedactMixin):
    key: str
    """
    Unique identifier of the property.

    Example: task.timeout
    """
    name: str | None = attr.field(default=None)
    """
    Human-readable name of the property.

    Example: Task timeout
    """
    description: str | None = attr.field(default=None)
    """
    Human-readable description of the property.

    Example: How long (in seconds) can individual task run without timing out.
    """
    value: str | None = attr.field(default=None)
    """
    Value of the property.

    Example: 3600
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/ExecutionParametersRunFacet.json#/$defs/ExecutionParameter"


@attr.define
class ExecutionParametersRunFacet(RunFacet):
    parameters: list[ExecutionParameter] | None = attr.field(factory=list)
    """The parameters passed to the Job at runtime"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/ExecutionParametersRunFacet.json#/$defs/ExecutionParametersRunFacet"
