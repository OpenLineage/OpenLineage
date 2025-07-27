# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage_sql import parse


def test_parse_small():
    metadata = parse(["SELECT * FROM test1"])
    assert len(metadata.in_tables) == 1
    assert metadata.in_tables[0].name == "test1"
    assert len(metadata.out_tables) == 0
