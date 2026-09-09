# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Tests for the dot-escaping utilities."""

import pytest
from openlineage.client.naming.escape import configure, escape, is_escaping_enabled


class TestIsEscapingEnabled:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        assert is_escaping_enabled() is False

    def test_enabled_when_true(self, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        assert is_escaping_enabled() is True

    def test_enabled_case_insensitive(self, monkeypatch):
        for value in ("true", "TRUE", "True", " true "):
            monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", value)
            assert is_escaping_enabled() is True, f"should be enabled for {value!r}"

    def test_disabled_for_non_true_values(self, monkeypatch):
        for value in ("false", "FALSE", "1", "yes", "on"):
            monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", value)
            assert is_escaping_enabled() is False, f"should be disabled for {value!r}"


class TestEscape:
    def test_plain_segment_unchanged(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        assert escape("plain") == "plain"
        assert escape("my_schema") == "my_schema"

    def test_segment_unchanged_by_default(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        assert escape("mydb.example.com") == "mydb.example.com"

    def test_single_dot_escaped_when_enabled(self, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        assert escape("a.b") == r"a\.b"

    def test_multiple_dots_escaped_when_enabled(self, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        assert escape("mydb.example.com") == r"mydb\.example\.com"
        assert escape("a.b.c") == r"a\.b\.c"

    def test_leading_trailing_dot_escaped_when_enabled(self, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        assert escape(".leading") == r"\.leading"
        assert escape("trailing.") == r"trailing\."


class TestEscapingIntegrationWithNaming:
    """Verify that escaping flows through the Naming helpers end-to-end."""

    def test_oracle_service_name_with_dots_unescaped_by_default(self, monkeypatch):
        from openlineage.client.naming.dataset import Oracle

        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        oracle = Oracle("localhost", "1521", "mydb.example.com", "mySchema", "myTable")
        assert oracle.get_name() == "mydb.example.com.mySchema.myTable"

    def test_oracle_service_name_with_dots_escaped_when_enabled(self, monkeypatch):
        from openlineage.client.naming.dataset import Oracle

        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        oracle = Oracle("localhost", "1521", "mydb.example.com", "mySchema", "myTable")
        assert oracle.get_name() == r"mydb\.example\.com.mySchema.myTable"

    def test_bigquery_project_id_with_dots_escaped_when_enabled(self, monkeypatch):
        from openlineage.client.naming.dataset import BigQuery

        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        bq = BigQuery("my.project.id", "dataset", "table")
        assert bq.get_name() == r"my\.project\.id.dataset.table"

    def test_postgres_plain_segments_unchanged(self, monkeypatch):
        from openlineage.client.naming.dataset import Postgres

        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        pg = Postgres("localhost", "5432", "mydb", "myschema", "mytable")
        assert pg.get_name() == "mydb.myschema.mytable"


class TestConfigure:
    """Tests for the configure() override that wires YAML config through."""

    @pytest.fixture(autouse=True)
    def reset_override(self):
        """Always reset the module-level override after each test."""
        yield
        configure(None)

    def test_configure_enables_escaping(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        configure(True)
        assert is_escaping_enabled() is True

    def test_configure_disables_escaping(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        configure(False)
        assert is_escaping_enabled() is False

    def test_configure_none_resets_to_env_var_true(self, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        configure(None)
        assert is_escaping_enabled() is True

    def test_configure_none_resets_to_env_var_false(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        configure(None)
        assert is_escaping_enabled() is False

    def test_configure_takes_precedence_over_env_var(self, monkeypatch):
        # Env var says "true" but configure says False → configure wins.
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        configure(False)
        assert is_escaping_enabled() is False

    def test_configure_false_takes_precedence_over_env_var_true(self, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        configure(False)
        assert escape("a.b") == "a.b"

    def test_configure_true_escapes_segment(self, monkeypatch):
        monkeypatch.delenv("OPENLINEAGE__NAME__ESCAPING", raising=False)
        configure(True)
        assert escape("mydb.example.com") == r"mydb\.example\.com"


class TestOpenLineageConfigNameParsing:
    """Verify that from_dict correctly parses the ``name`` section."""

    def test_name_escaping_true_parsed(self):
        from openlineage.client.client import OpenLineageConfig

        cfg = OpenLineageConfig.from_dict({"name": {"escaping": True}})
        assert cfg.name.escaping is True

    def test_name_escaping_false_parsed(self):
        from openlineage.client.client import OpenLineageConfig

        cfg = OpenLineageConfig.from_dict({"name": {"escaping": False}})
        assert cfg.name.escaping is False

    def test_name_absent_defaults_to_none(self):
        from openlineage.client.client import OpenLineageConfig

        cfg = OpenLineageConfig.from_dict({})
        assert cfg.name.escaping is None

    def test_name_escaping_absent_defaults_to_none(self):
        from openlineage.client.client import OpenLineageConfig

        cfg = OpenLineageConfig.from_dict({"name": {}})
        assert cfg.name.escaping is None
