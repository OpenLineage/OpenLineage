# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Long-running worker: runs the task on a cron schedule until signalled to stop.

Foreground process (Cloud Foundry's Diego runtime supervises/restarts it -- do not self-daemonize).
Start with:  exec python -m app.worker

Handles SIGTERM/SIGINT for graceful shutdown: it stops scheduling new runs and lets an in-flight run
finish. A currently-running scan is not interrupted (jobs are meant to be small and idempotent).
"""

from __future__ import annotations

import logging
import signal
import threading

from app.config import load_settings
from app.logging import configure_logging
from app.scheduler import CronScheduler
from app.secrets import get_secrets_provider
from app.tasks.registry import TASKS

log = logging.getLogger(__name__)

# The single task this worker runs today.
_TASK_NAME = "odata_to_ol"


def main() -> int:
    settings = load_settings()
    configure_logging(settings.logging.level)
    secrets = get_secrets_provider()
    task = TASKS[_TASK_NAME]

    stop = threading.Event()

    def _handle(signum, _frame):
        log.info("shutdown signal received", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    def job() -> None:
        try:
            summary = task(settings, secrets)
            log.info("scheduled run complete", extra={"task": _TASK_NAME, "summary": summary})
        except Exception:
            # Never let a single failed run kill the worker; Diego would restart it anyway.
            log.exception("scheduled run failed", extra={"task": _TASK_NAME})

    scheduler = CronScheduler(settings.scheduler.cron, settings.scheduler.timezone)
    log.info("worker started", extra={"task": _TASK_NAME, "cron": settings.scheduler.cron})
    scheduler.run(job, stop)
    log.info("worker exiting")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
