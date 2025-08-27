# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import RunFacet


@attr.define
class NominalTimeRunFacet(RunFacet):
    nominalStartTime: str = attr.field()  # noqa: N815
    """
    An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal start time
    (included) of the run. AKA the schedule time
    """
    nominalEndTime: str | None = attr.field(default=None)  # noqa: N815
    """
    An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp representing the nominal end time
    (excluded) of the run. (Should be the nominal start time of the next run)
    """
    _additional_skip_redact: ClassVar[list[str]] = ["nominalStartTime", "nominalEndTime"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json#/$defs/NominalTimeRunFacet"

    @nominalStartTime.validator
    def nominalstarttime_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from dateutil import parser

        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)

    @nominalEndTime.validator
    def nominalendtime_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        if value is None:
            return
        from dateutil import parser

        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)
