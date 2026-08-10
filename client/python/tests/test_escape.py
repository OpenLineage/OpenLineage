# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Tests for the dot-escaping utilities."""

from openlineage.client.naming.escape import escape, is_escaping_enabled


class TestIsEscapingEnabled:
    def test_enabled_by_default(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        assert is_escaping_enabled() is True

    def test_disabled_when_false(self, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "false")
        assert is_escaping_enabled() is False

    def test_disabled_case_insensitive(self, monkeypatch):
        for value in ("false", "FALSE", "False", " false "):
            monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", value)
            assert is_escaping_enabled() is False, f"should be disabled for {value!r}"

    def test_enabled_for_true_values(self, monkeypatch):
        for value in ("true", "TRUE", "1", "yes", "on"):
            monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", value)
            assert is_escaping_enabled() is True, f"should be enabled for {value!r}"


class TestEscape:
    def test_plain_segment_unchanged(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        assert escape("plain") == "plain"
        assert escape("my_schema") == "my_schema"

    def test_single_dot_escaped(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        assert escape("a.b") == r"a\.b"

    def test_multiple_dots_escaped(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        assert escape("mydb.example.com") == r"mydb\.example\.com"
        assert escape("a.b.c") == r"a\.b\.c"

    def test_segment_unchanged_when_escaping_disabled(self, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "false")
        assert escape("mydb.example.com") == "mydb.example.com"

    def test_leading_trailing_dot(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        assert escape(".leading") == r"\.leading"
        assert escape("trailing.") == r"trailing\."


class TestEscapingIntegrationWithNaming:
    """Verify that escaping flows through the Naming helpers end-to-end."""

    def test_oracle_service_name_with_dots_escaped(self, monkeypatch):
        from openlineage.client.naming.dataset import Oracle

        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        oracle = Oracle("localhost", "1521", "mydb.example.com", "mySchema", "myTable")
        assert oracle.get_name() == r"mydb\.example\.com.mySchema.myTable"

    def test_oracle_service_name_with_dots_unescaped_when_disabled(self, monkeypatch):
        from openlineage.client.naming.dataset import Oracle

        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "false")
        oracle = Oracle("localhost", "1521", "mydb.example.com", "mySchema", "myTable")
        assert oracle.get_name() == "mydb.example.com.mySchema.myTable"

    def test_bigquery_project_id_with_dots_escaped(self, monkeypatch):
        from openlineage.client.naming.dataset import BigQuery

        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        bq = BigQuery("my.project.id", "dataset", "table")
        assert bq.get_name() == r"my\.project\.id.dataset.table"

    def test_postgres_plain_segments_unchanged(self, monkeypatch):
        from openlineage.client.naming.dataset import Postgres

        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        pg = Postgres("localhost", "5432", "mydb", "myschema", "mytable")
        assert pg.get_name() == "mydb.myschema.mytable"
