# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import threading
from datetime import datetime, timezone

import pytest

from app.scheduler import CronScheduler


def test_invalid_cron_raises():
    with pytest.raises(ValueError):
        CronScheduler("not a cron")


def test_next_run_is_in_future():
    sched = CronScheduler("*/5 * * * *", "UTC")
    base = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
    nxt = sched.next_run(base)
    assert nxt == datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)


def test_run_exits_promptly_when_stopped_before_first_tick():
    # A cron far in the future; a pre-set stop event must cause immediate exit.
    sched = CronScheduler("0 0 1 1 *", "UTC")
    stop = threading.Event()
    stop.set()
    ran = []
    sched.run(lambda: ran.append(1), stop)
    assert ran == []
