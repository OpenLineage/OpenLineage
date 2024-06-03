# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage_sql import parse


def test_parse_small():
    metadata = parse(["select * from test_orders as test_orders"])
    assert metadata.inputs == ["test1"]
    assert metadata.output is None


test_parse_small()
