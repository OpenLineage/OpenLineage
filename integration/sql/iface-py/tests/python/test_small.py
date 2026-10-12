# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest
from openlineage_sql import parse


def test_parse_small():
    metadata = parse(["SELECT * FROM test1"])
    assert len(metadata.in_tables) == 1
    assert metadata.in_tables[0].name == "test1"
    assert len(metadata.out_tables) == 0


@pytest.mark.parametrize(
    ("sql", "columns"),
    [
        ("SELECT COUNT(* WHERE status = 'paid') AS result FROM source", ["status"]),
        ("SELECT payload IS JSON AS result FROM source", ["payload"]),
        (
            "SELECT text_col LIKE pattern_col ESCAPE escape_col AS result FROM source",
            ["escape_col", "pattern_col", "text_col"],
        ),
    ],
)
def test_new_expression_lineage(sql, columns):
    metadata = parse([sql], dialect="postgres")
    assert metadata.errors == []
    assert len(metadata.column_lineage) == 1
    lineage = metadata.column_lineage[0]
    assert lineage.descendant.name == "result"
    assert [column.name for column in lineage.lineage] == columns
    assert all(column.origin.name == "source" for column in lineage.lineage)


def test_unsupported_pipe_query():
    with pytest.raises(RuntimeError, match="SQL pipe operators are not supported"):
        parse(
            ["FROM orders |> JOIN customers ON orders.customer_id = customers.id |> SELECT orders.id"],
            dialect="databricks",
        )
