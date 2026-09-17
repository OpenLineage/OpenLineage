# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import textwrap

from app.config import load_settings
from app.datasphere.models import Space
from app.tasks.odata_to_ol import resolve_enabled_spaces


def _write(tmp_path, body):
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(body))
    return str(p)


def test_load_and_defaults(tmp_path):
    path = _write(
        tmp_path,
        """
        datasphere:
          base_url: https://tenant.example
          tenant_host: tenant.example
        """,
    )
    s = load_settings(path)
    assert s.datasphere.odata_base_path == "/dwaas-core/odata/v4"  # default
    assert s.datasphere.auth.type == "cookie"  # default
    assert s.openlineage.transport == {"type": "console"}  # default
    assert s.scheduler.cron == "0 * * * *"  # default


def test_env_override(tmp_path, monkeypatch):
    path = _write(
        tmp_path,
        """
        datasphere:
          base_url: https://tenant.example
          tenant_host: tenant.example
          request_timeout_seconds: 10
        """,
    )
    monkeypatch.setenv("DS_OL__DATASPHERE__REQUEST_TIMEOUT_SECONDS", "42")
    monkeypatch.setenv("DS_OL__SCHEDULER__CRON", "*/5 * * * *")
    s = load_settings(path)
    assert s.datasphere.request_timeout_seconds == 42
    assert s.scheduler.cron == "*/5 * * * *"


def test_resolve_enabled_spaces(tmp_path):
    spaces = [Space("A"), Space("B"), Space("C")]

    class Cfg:
        include = ["A", "B"]
        exclude = ["B"]

    assert [s.id for s in resolve_enabled_spaces(spaces, Cfg())] == ["A"]

    class All:
        include = []
        exclude = []

    assert [s.id for s in resolve_enabled_spaces(spaces, All())] == ["A", "B", "C"]
