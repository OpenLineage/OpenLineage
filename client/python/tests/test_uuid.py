# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from openlineage.client.uuid import generate_new_uuid, generate_static_uuid


def test_generate_new_uuid_returns_uuidv7():
    assert generate_new_uuid().version == 7  # noqa: PLR2004


def test_generate_new_uuid_returns_different_result_on_each_call():
    result1 = generate_new_uuid()
    result2 = generate_new_uuid()
    assert result1 != result2

    instant = datetime.now()
    result1 = generate_new_uuid(instant)
    result2 = generate_new_uuid(instant)
    assert result1 != result2


def test_generate_new_uuid_result_is_increasing_with_instant_increment():
    result1 = generate_new_uuid(datetime.now())
    result2 = generate_new_uuid(datetime.now() + timedelta(seconds=0.001))
    assert result1 != result2
    assert result1 < result2


def test_generate_new_uuid_returns_prefix_based_on_instant_milliseconds():
    instant_milliseconds = datetime(2025, 5, 20, 10, 52, 33, 881000, tzinfo=timezone.utc)
    instant_microseconds = datetime(2025, 5, 20, 10, 52, 33, 881863, tzinfo=timezone.utc)

    uuid1 = generate_new_uuid(instant_milliseconds)
    uuid2 = generate_new_uuid(instant_microseconds)

    assert str(uuid1).startswith("0196ed52-e0d9-7")
    assert str(uuid2).startswith("0196ed52-e0d9-7")


def test_generate_static_uuid_returns_uuidv7():
    assert generate_static_uuid(datetime.now(), b"some").version == 7  # noqa: PLR2004


def test_generate_static_uuid_returns_same_result_for_same_input():
    instant = datetime.now()
    data = b"some"
    assert generate_static_uuid(instant, data) == generate_static_uuid(instant, data)


@pytest.mark.parametrize(
    "data",
    [
        b"some",
        b"other",
    ],
)
def test_generate_static_uuid_result_prefix_depends_on_instant_milliseconds(data):
    instant_milliseconds = datetime(2025, 5, 20, 10, 52, 33, 881000, tzinfo=timezone.utc)
    instant_microseconds = datetime(2025, 5, 20, 10, 52, 33, 881863, tzinfo=timezone.utc)

    uuid1 = generate_static_uuid(instant_milliseconds, data)
    uuid2 = generate_static_uuid(instant_microseconds, data)

    assert str(uuid1).startswith("0196ed52-e0d9-7")
    assert str(uuid2).startswith("0196ed52-e0d9-7")


@pytest.mark.parametrize(
    ("data1", "data2"),
    [
        (b"some", b"other"),
        (b"some", b"some"),
    ],
)
def test_generate_static_uuid_result_is_increasing_with_instant_increment(data1, data2):
    instant = datetime.now()
    result1 = generate_static_uuid(instant, data1)
    result2 = generate_static_uuid(instant + timedelta(seconds=0.001), data2)
    assert result1 != result2
    assert result1 < result2

    # both timestamp and random parts are different
    assert result1.bytes != result2.bytes
    assert result1.bytes_le != result2.bytes_le


def test_generate_static_uuid_result_is_different_for_different_data():
    timestamp = datetime.now()
    result1 = generate_static_uuid(timestamp, b"some")
    result2 = generate_static_uuid(timestamp, b"other")
    assert result1 != result2

    # timestamp part is the same
    assert int.from_bytes(result1.bytes, "big") & 0x0000 == int.from_bytes(result2.bytes, "big") & 0x0000

    # random part is different
    assert int.from_bytes(result1.bytes, "big") & 0xFFFF != int.from_bytes(result2.bytes, "big") & 0xFFFF
    assert result1.bytes_le != result2.bytes_le
