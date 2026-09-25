# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Registry of runnable tasks, keyed by the name used on the CLI / by the scheduler."""

from __future__ import annotations

from collections.abc import Callable

from app.tasks.odata_to_ol import run_odata_to_ol

# Each task: (settings, secrets) -> summary dict
TASKS: dict[str, Callable] = {
    "odata_to_ol": run_odata_to_ol,
}
