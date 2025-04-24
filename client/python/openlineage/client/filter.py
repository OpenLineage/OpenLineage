# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import re
import typing

import attr
from openlineage.client.event_v2 import RunEvent as RunEvent_v2
from openlineage.client.run import RunEvent

log = logging.getLogger(__name__)
RunEventType = typing.Union[RunEvent, RunEvent_v2]


@attr.s
class FilterConfig:
    type: str | None = attr.ib(default=None)
    match: str | None = attr.ib(default=None)
    regex: str | None = attr.ib(default=None)


class Filter:
    def filter_event(self, event: RunEventType) -> RunEventType | None:
        ...


class ExactMatchFilter(Filter):
    def __init__(self, match: str) -> None:
        self.match = match

    def filter_event(self, event: RunEventType) -> RunEventType | None:
        if self.match == event.job.name:
            log.debug("Job name `%s` matches exactly `%s`.", event.job.name, self.match)
            return None
        return event


class RegexFilter(Filter):
    def __init__(self, regex: str) -> None:
        self.pattern = re.compile(regex)

    def filter_event(self, event: RunEventType) -> RunEventType | None:
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
