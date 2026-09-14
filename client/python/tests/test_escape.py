# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Tests for the dot-escaping utilities."""

import pytest
from openlineage.client.naming.escape import configure, escape, is_escaping_enabled


def _split_escaped(name: str) -> list[str]:
    """Split a dot-separated OL name back into segments, honouring the escape grammar.

    ``\\.`` is a literal dot, ``\\\\`` is a literal backslash, and an unescaped
    ``.`` is a structural separator.  Used only in round-trip tests.
    """
    segments: list[str] = []
    current: list[str] = []
    i = 0
    while i < len(name):
        ch = name[i]
        if ch == "\\" and i + 1 < len(name):
            # escape sequence: consume backslash and emit next char literally
            current.append(name[i + 1])
            i += 2
        elif ch == ".":
            # structural separator
            segments.append("".join(current))
            current = []
            i += 1
        else:
            current.append(ch)
            i += 1
    segments.append("".join(current))
    return segments


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

    def test_backslash_escaped_before_dot(self, monkeypatch):
        # A literal backslash in the segment must be doubled before dots are
        # escaped, otherwise consumers cannot distinguish an escaped dot from a
        # literal backslash-dot sequence.
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")
        # "foo\.bar"  (backslash + dot)  →  "foo\\.bar"  (each \ → \\, then . → \.)
        assert escape("foo\\.bar") == "foo\\\\\\.bar"
        # "foo\bar"  (backslash, no dot)  →  "foo\\bar"
        assert escape("foo\\bar") == "foo\\\\bar"
        # plain backslash at end
        assert escape("foo\\") == "foo\\\\"

    def test_special_combinations(self, monkeypatch):
        # Round-trip test: for each input name (list of segments), escape every
        # segment, join with '.', parse back using the same grammar, and verify
        # the recovered segments match the originals.
        #
        # This validates the encoder is unambiguous — no special-character
        # combination in a segment value can be confused with a structural dot
        # or a misinterpreted escape sequence by the consumer.
        monkeypatch.setenv("OPENLINEAGE__NAME__ESCAPING", "true")

        cases = [
            # ── single-segment names (no structural dot at all) ──────────────
            [""],  # empty segment
            ["plain"],
            ["my_schema-1"],
            ["tëst"],  # non-ASCII passthrough
            ["a/b:c"],  # other punctuation
            ["."],  # segment is just a dot
            ["..."],  # segment is only dots
            [".a."],  # dot-wrapped word
            ["\\"],  # single backslash
            ["\\\\"],  # two backslashes
            ["\\\\\\"],  # three backslashes
            ["\\."],  # backslash immediately before dot
            ["a\\.b"],  # backslash+dot mid-segment
            ["\\\\."],  # two backslashes then dot
            [".\\"],  # dot then backslash
            ["\\.\\"],  # backslash dot backslash
            ["a.b\\c"],  # mid-dot and trailing backslash
            ["\\a.b"],  # leading backslash then mid-dot
            # ── multi-segment names (structural dots present) ────────────────
            ["mydb.example.com", "mySchema", "myTable"],
            ["a.b.c", "d.e"],  # dots in both segments
            [".", ".", "."],  # every segment is a bare dot
            ["\\.\\", "plain"],  # complex + plain
            ["foo", "bar\\.baz", "qux"],  # backslash+dot in middle segment
        ]

        for segments in cases:
            encoded = ".".join(escape(s) for s in segments)
            recovered = _split_escaped(encoded)
            assert recovered == segments, f"round-trip failed for {segments!r} (encoded: {encoded!r})"


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
