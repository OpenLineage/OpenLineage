# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage_sql import parse


def test_parse_small():
    metadata = parse("SELECT * FROM test1")
    assert metadata.inputs == ["test1"]
    assert metadata.output is None

