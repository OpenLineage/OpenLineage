# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

import attr

if TYPE_CHECKING:
    from openlineage.client.event_v2 import RunEvent

log = logging.getLogger(__name__)


@attr.define
class FilterConfig:
    type: str | None = None
    match: str | None = None
    regex: str | None = None


class Filter:
    def filter_event(self, event: RunEvent) -> RunEvent | None:
        ...


class ExactMatchFilter(Filter):
    def __init__(self, match: str) -> None:
        self.match = match

    def filter_event(self, event: RunEvent) -> RunEvent | None:
        if self.match == event.job.name:
            log.debug("Job name `%s` matches exactly `%s`.", event.job.name, self.match)
            return None
        return event


class RegexFilter(Filter):
    def __init__(self, regex: str) -> None:
        self.pattern = re.compile(regex)

    def filter_event(self, event: RunEvent) -> RunEvent | None:
        if self.pattern.match(event.job.name):
            log.debug("Job name `%s` matches regex `%s`.", event.job.name, self.pattern.pattern)
            return None
        return event


def create_filter(conf: FilterConfig) -> Filter | None:
    if not conf.type:
        log.warning("OpenLineage filter config must have a `type`.")
        return None
    if conf.type == "exact" and conf.match:
        log.debug("Creating ExactMatchFilter with match='%s'", conf.match)
        return ExactMatchFilter(match=conf.match)
    if conf.type == "regex" and conf.regex:
        log.debug("Creating RegexFilter with regex='%s'", conf.regex)
        return RegexFilter(regex=conf.regex)
    log.warning("Unsupported OpenLineage filter type: `%s`", conf.type)
    return None
