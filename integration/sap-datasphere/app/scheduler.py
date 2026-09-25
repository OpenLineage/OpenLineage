# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Minimal cron scheduler.

Recomputes the next fire time from the current time after each run, so a slow job never causes a
pileup or overlap -- missed ticks are simply skipped. Times are handled in UTC by default (Cloud
Foundry containers run UTC). Uses a ``threading.Event`` for prompt, cooperative shutdown.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter

log = logging.getLogger(__name__)


class CronScheduler:
    def __init__(self, cron_expr: str, timezone: str = "UTC") -> None:
        self._cron_expr = cron_expr
        self._tz = ZoneInfo(timezone)
        # Validate the expression eagerly.
        if not croniter.is_valid(cron_expr):
            raise ValueError(f"Invalid cron expression: {cron_expr!r}")

    def next_run(self, after: datetime | None = None) -> datetime:
        base = after or datetime.now(self._tz)
        return croniter(self._cron_expr, base).get_next(datetime)

    def run(self, job: Callable[[], None], stop: threading.Event) -> None:
        log.info("scheduler started", extra={"cron": self._cron_expr, "tz": str(self._tz)})
        while not stop.is_set():
            now = datetime.now(self._tz)
            nxt = self.next_run(now)
            wait_seconds = max(0.0, (nxt - now).total_seconds())
            log.info("next run scheduled", extra={"at": nxt.isoformat(), "in_seconds": round(wait_seconds, 1)})
            if stop.wait(wait_seconds):
                break
            job()
        log.info("scheduler stopped")
