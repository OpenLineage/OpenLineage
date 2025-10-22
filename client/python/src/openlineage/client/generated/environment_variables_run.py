# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class EnvironmentVariable(RedactMixin):
    name: str
    """The name of the environment variable."""

    value: str
    """The value of the environment variable."""

    _skip_redact: ClassVar[list[str]] = ["name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/EnvironmentVariablesRunFacet.json#/$defs/EnvironmentVariable"


@attr.define
class EnvironmentVariablesRunFacet(RunFacet):
    environmentVariables: list[EnvironmentVariable]  # noqa: N815
    """The environment variables for the run."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/EnvironmentVariablesRunFacet.json#/$defs/EnvironmentVariablesRunFacet"
