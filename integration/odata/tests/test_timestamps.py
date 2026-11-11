# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import timezone

from app.datasphere.timestamps import parse_datasphere_timestamp, parse_iso_timestamp


def test_parse_repo_timestamp_nanoseconds_and_zone():
    dt = parse_datasphere_timestamp("2025-08-29 21:39:47.050000000 UTC")
    assert dt is not None
    assert (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) == (2025, 8, 29, 21, 39, 47)
    assert dt.tzinfo == timezone.utc


def test_parse_repo_timestamp_without_fraction():
    dt = parse_datasphere_timestamp("2025-01-02 03:04:05 UTC")
    assert dt is not None and dt.minute == 4


def test_parse_none_and_empty():
    assert parse_datasphere_timestamp(None) is None
    assert parse_datasphere_timestamp("") is None
    assert parse_iso_timestamp(None) is None


def test_parse_iso_naive_becomes_utc():
    dt = parse_iso_timestamp("2026-09-10T08:42:00")
    assert dt is not None and dt.tzinfo == timezone.utc


def test_parse_iso_date_only():
    dt = parse_iso_timestamp("2015-01-01")
    assert dt is not None and dt.year == 2015
