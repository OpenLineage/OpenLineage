# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from openlineage.client.uuid import generate_new_uuid, generate_static_uuid


def test_generate_new_uuid_returns_uuidv7():
    assert generate_new_uuid().version == 7  # noqa: PLR2004


def test_generate_static_uuid_returns_different_result_on_each_call():
    instant = datetime.now()
    result1 = generate_new_uuid(instant)
    result2 = generate_new_uuid(instant)
    assert result1 != result2


def test_generate_new_uuid_result_is_increasing_with_instant_increment():
    result1 = generate_new_uuid(datetime.now())
    result2 = generate_new_uuid(datetime.now() + timedelta(seconds=0.001))
    assert result1 != result2
    assert result1 < result2


def test_generate_static_uuid_returns_uuidv7():
    assert generate_static_uuid(datetime.now(), b"some").version == 7  # noqa: PLR2004


def test_generate_static_uuid_returns_same_result_for_same_input():
    instant = datetime.now()
    data = b"some"
    assert generate_static_uuid(instant, data) == generate_static_uuid(instant, data)


@pytest.mark.parametrize(
    ("data1", "data2"),
    [
        (b"some", b"other"),
        (b"some", b"some"),
    ],
)
def test_generate_static_uuid_result_is_increasing_with_instant_increment(data1, data2):
    result1 = generate_static_uuid(datetime.now(), data1)
    result2 = generate_static_uuid(datetime.now() + timedelta(seconds=0.001), data2)
    assert result1 != result2
    assert result1 < result2
