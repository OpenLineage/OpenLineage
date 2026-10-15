# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest
from openlineage_sql import parse


def test_parse_small():
    metadata = parse(["SELECT * FROM test1"])
    assert len(metadata.in_tables) == 1
    assert metadata.in_tables[0].name == "test1"
    assert len(metadata.out_tables) == 0


def test_default_database_qualifies_table_and_column_lineage():
    metadata = parse(
        ["INSERT INTO target SELECT id FROM source"],
        dialect="postgres",
        default_schema="public",
        default_database="mydb",
    )
    assert metadata.errors == []
    source, target = metadata.in_tables[0], metadata.out_tables[0]
    assert (source.database, source.schema, source.name) == ("mydb", "public", "source")
    assert (target.database, target.schema, target.name) == ("mydb", "public", "target")
    assert len(metadata.column_lineage) == 1
    lineage = metadata.column_lineage[0]
    assert lineage.lineage[0].origin == source
    assert lineage.descendant.name == "id"
    assert lineage.descendant.origin is None


@pytest.mark.parametrize(
    ("sql", "expected_database", "expected_schema", "expected_name"),
    [
        ("SELECT id FROM custom.source", "mydb", "custom", "source"),
        ("SELECT id FROM otherdb.custom.source", "otherdb", "custom", "source"),
        ('SELECT id FROM "OtherDb"."Custom"."Source"', "OtherDb", "Custom", "Source"),
    ],
)
def test_explicit_names_override_defaults(sql, expected_database, expected_schema, expected_name):
    metadata = parse(
        [sql],
        dialect="postgres",
        default_schema="public",
        default_database="mydb",
    )
    source = metadata.in_tables[0]
    assert (source.database, source.schema, source.name) == (
        expected_database,
        expected_schema,
        expected_name,
    )
    assert metadata.column_lineage[0].lineage[0].origin == source


def test_optional_default_database_and_independent_calls():
    sql = ["SELECT id FROM source"]
    omitted = parse(sql, dialect="postgres", default_schema="public")
    explicit_none = parse(sql, "postgres", "public", None)
    assert omitted.in_tables == explicit_none.in_tables
    assert omitted.column_lineage == explicit_none.column_lineage
    assert omitted.in_tables[0].database is None
    for database in ("FirstDb", "SecondDb"):
        metadata = parse(sql, "postgres", "public", database)
        assert metadata.in_tables[0].database == database
        assert metadata.column_lineage[0].lineage[0].origin == metadata.in_tables[0]
    assert parse(sql, "postgres", "public").in_tables[0].database is None


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
