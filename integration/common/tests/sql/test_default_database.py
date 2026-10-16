# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import Mock

import pytest
from openlineage.common.sql import parse


@pytest.mark.parametrize("as_list", [False, True])
def test_default_database_qualifies_table_and_column_lineage(as_list):
    sql = "INSERT INTO target SELECT id FROM source"
    metadata = parse(
        [sql] if as_list else sql,
        dialect="postgres",
        default_schema="public",
        default_database="mydb",
    )

    assert metadata is not None
    assert metadata.errors == []
    source, target = metadata.in_tables[0], metadata.out_tables[0]
    assert (source.database, source.schema, source.name) == ("mydb", "public", "source")
    assert (target.database, target.schema, target.name) == ("mydb", "public", "target")
    assert metadata.column_lineage[0].lineage[0].origin == source


@pytest.mark.parametrize(
    ("sql", "expected_database", "expected_schema"),
    [
        ("SELECT id FROM custom.source", "mydb", "custom"),
        ("SELECT id FROM otherdb.custom.source", "otherdb", "custom"),
        ("USE otherdb; SELECT id FROM source", "otherdb", "public"),
    ],
)
def test_sql_context_overrides_defaults(sql, expected_database, expected_schema):
    metadata = parse(sql, "generic", "public", "mydb")

    assert metadata is not None
    assert metadata.errors == []
    source = metadata.in_tables[0]
    assert (source.database, source.schema, source.name) == (
        expected_database,
        expected_schema,
        "source",
    )
    assert metadata.column_lineage[0].lineage[0].origin == source


def test_default_database_is_scoped_to_each_call():
    sql = "SELECT id FROM source"
    for database in ("FirstDb", "SecondDb"):
        metadata = parse(sql, "postgres", "public", database)
        assert metadata is not None
        assert metadata.in_tables[0].database == database
        assert metadata.column_lineage[0].lineage[0].origin == metadata.in_tables[0]

    omitted = parse(sql, "postgres", "public")
    explicit_none = parse(sql, "postgres", "public", None)
    assert omitted is not None
    assert explicit_none is not None
    assert omitted.in_tables == explicit_none.in_tables
    assert omitted.column_lineage == explicit_none.column_lineage
    assert omitted.in_tables[0].database is None


@pytest.mark.parametrize("explicit_none", [False, True])
def test_omitted_database_retains_legacy_native_signature(monkeypatch, explicit_none):
    # Existing integrations can still use a native parser predating the fourth argument.
    result = object()

    def legacy_parse(sql, dialect=None, default_schema=None):
        assert sql == ["SELECT id FROM source"]
        assert dialect == "postgres"
        assert default_schema == "public"
        return result

    monkeypatch.setattr("openlineage.common.sql.parse_sql", legacy_parse)
    kwargs = {"default_database": None} if explicit_none else {}
    assert parse("SELECT id FROM source", "postgres", "public", **kwargs) is result


def test_native_failure_with_database_is_logged_and_returns_none(monkeypatch, caplog):
    native_parse = Mock(side_effect=RuntimeError("native parser failure"))
    monkeypatch.setattr("openlineage.common.sql.parse_sql", native_parse)

    assert parse("SELECT id FROM source", "postgres", "public", "mydb") is None
    assert "SQL parser failed: native parser failure" in caplog.text
    native_parse.assert_called_once_with(
        ["SELECT id FROM source"],
        dialect="postgres",
        default_schema="public",
        default_database="mydb",
    )
