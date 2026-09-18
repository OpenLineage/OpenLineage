# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""One-shot task entrypoint.

Runs a single task once and exits. This is both the local "run once" command and the exact command
a Cloud Foundry task / SAP Job Scheduling Service job invokes:

    python -m app.run_task odata_to_ol
"""

from __future__ import annotations

import argparse
import logging
import sys

from app.config import load_settings
from app.logging import configure_logging
from app.secrets import get_secrets_provider
from app.tasks.registry import TASKS

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a single SAP Datasphere -> OpenLineage task.")
    parser.add_argument("task", nargs="?", default="odata_to_ol", choices=sorted(TASKS))
    parser.add_argument("--config", help="Path to config YAML (overrides DS_OL_CONFIG).")
    args = parser.parse_args(argv)

    settings = load_settings(args.config)
    configure_logging(settings.logging.level)
    secrets = get_secrets_provider()

    task = TASKS[args.task]
    log.info("task starting", extra={"task": args.task})
    try:
        summary = task(settings, secrets)
    except Exception:
        log.exception("task failed", extra={"task": args.task})
        return 1
    log.info("task finished", extra={"task": args.task, "summary": summary})
    return 0


if __name__ == "__main__":
    sys.exit(main())
