# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Timestamp parsing helpers for the various formats Datasphere returns."""

from __future__ import annotations

import re
from datetime import datetime, timezone

from dateutil import parser as _dateutil_parser

# e.g. "2025-08-29 21:39:47.050000000 UTC"  (space-separated, up-to-9-digit fraction, trailing zone)
_DS_REPO_RE = re.compile(
    r"^(?P<body>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})(?:\.(?P<frac>\d+))?\s*(?P<zone>[A-Za-z]+)?$"
)


def parse_datasphere_timestamp(value: str | None) -> datetime | None:
    """Parse repository ``deployment_date``/``modification_date`` strings. Assumes UTC."""
    if not value:
        return None
    value = value.strip()
    m = _DS_REPO_RE.match(value)
    if m:
        body = m.group("body").replace("T", " ")
        frac = (m.group("frac") or "")[:6].ljust(6, "0") if m.group("frac") else "000000"
        try:
            dt = datetime.strptime(f"{body}.{frac}", "%Y-%m-%d %H:%M:%S.%f")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return parse_iso_timestamp(value)


def parse_iso_timestamp(value: str | None) -> datetime | None:
    """Parse an ISO-8601 (or loosely ISO) timestamp/date. Naive values are treated as UTC."""
    if not value:
        return None
    try:
        dt = _dateutil_parser.parse(str(value))
    except (ValueError, OverflowError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
