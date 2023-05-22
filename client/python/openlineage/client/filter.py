# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
import typing

if typing.TYPE_CHECKING:
    from openlineage.client.run import RunEvent


class Filter:
    def filter_event(self, event: RunEvent) -> RunEvent | None:
        ...


class ExactMatchFilter(Filter):
    def __init__(self, match: str) -> None:
        self.match = match

    def filter_event(self, event: RunEvent) -> RunEvent | None:
        if self.match == event.job.name:
            return None
        return event


class RegexFilter(Filter):
    def __init__(self, regex: str) -> None:
        self.pattern = re.compile(regex)

    def filter_event(self, event: RunEvent) -> RunEvent | None:
        if self.pattern.match(event.job.name):
            return None
        return event


def create_filter(conf: dict[str, str]) -> Filter | None:
    if "type" not in conf:
        return None
    # Switch in 3.10 🙂
    if conf["type"] == "exact":
        return ExactMatchFilter(match=conf["match"])
    if conf["type"] == "regex":
        return RegexFilter(regex=conf["regex"])
    return None
