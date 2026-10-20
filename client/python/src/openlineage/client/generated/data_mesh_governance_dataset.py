# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum

import attr
from openlineage.client.utils import RedactMixin


class DataClassification(Enum):
    """Information sensitivity tier."""

    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"


@attr.define
class PolicyCheck(RedactMixin):
    policyName: str  # noqa: N815
    engine: str
    passed: bool
    evaluatedAt: str | None = attr.field(default=None)  # noqa: N815
    severity: Severity | None = attr.field(default=None)
    details: str | None = attr.field(default=None)

    @evaluatedAt.validator
    def evaluatedat_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        if value is None:
            return
        from dateutil import parser

        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)


class Severity(Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
