# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class JobRunParameterFields(RedactMixin):
    key: str
    """
    Unique identifier for the property.

    Example: airflow.dagRun.external_trigger
    """
    name: str | None = attr.field(default=None)
    """
    Human-readable name of the property.

    Example: External trigger
    """
    description: str | None = attr.field(default=None)
    """Detailed explanation of the property."""

    value: str | float | bool | dict[str, Any] | list[Any] | None = attr.field(factory=list)
    """Value of the property; can be any JSON type."""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-0/JobRunParametersFacet.json#/$defs/JobRunParameterFields"
        )


@attr.define
class JobRunParametersFacet(RunFacet):
    parameters: list[JobRunParameterFields] | None = attr.field(factory=list)
    """The parameters passed to the Job at runtime"""

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://openlineage.io/spec/facets/1-0-0/JobRunParametersFacet.json#/$defs/JobRunParametersFacet"
        )
