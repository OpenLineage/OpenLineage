# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Pluggable resolvers for a dataset's "last time data was written" timestamp.

Strategies are tried in the configured order; the first that yields a value wins. See
:mod:`app.lastupdated.base` for the interface and the individual modules for each strategy.
"""

from __future__ import annotations

from app.lastupdated.base import LastUpdated, LastUpdatedResolver
from app.lastupdated.design_time import DesignTimeResolver
from app.lastupdated.max_timestamp_column import MaxTimestampColumnResolver
from app.lastupdated.monitoring import MonitoringResolver

_REGISTRY = {
    "monitoring": MonitoringResolver,
    "max_timestamp_column": MaxTimestampColumnResolver,
    "design_time": DesignTimeResolver,
}


def build_resolvers(config) -> list[LastUpdatedResolver]:
    resolvers: list[LastUpdatedResolver] = []
    for name in config.strategies:
        factory = _REGISTRY.get(name)
        if factory is None:
            raise ValueError(f"Unknown lastupdated strategy: {name!r}")
        if name == "max_timestamp_column":
            resolvers.append(factory(config.timestamp_column_patterns))
        else:
            resolvers.append(factory())
    return resolvers


__all__ = ["LastUpdated", "LastUpdatedResolver", "build_resolvers"]
