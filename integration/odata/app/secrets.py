# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Secret resolution seam.

Business code never reads secrets directly; it asks a ``SecretsProvider`` for a key. The same
key names work locally and in Cloud Foundry -- only the *source* differs:

* Local: environment variables (optionally seeded from a ``.env`` file).
* Cloud Foundry: ``VCAP_SERVICES`` (bound services / user-provided services), read via ``cfenv``.

The provider is chosen by :func:`get_secrets_provider`, which branches on ``VCAP_APPLICATION``
(present only inside a Cloud Foundry container).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol


class SecretsProvider(Protocol):
    def get(self, key: str, default: str | None = None) -> str | None: ...


def _load_dotenv(path: Path) -> None:
    """Minimal ``.env`` loader (no external dependency). Does not override existing env vars."""
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


class EnvSecretsProvider:
    """Reads secrets from environment variables, seeding from ``.env`` files if present."""

    def __init__(self, dotenv_paths: list[Path] | None = None) -> None:
        for p in dotenv_paths or [Path(".env"), Path("config/.env")]:
            _load_dotenv(p)

    def get(self, key: str, default: str | None = None) -> str | None:
        return os.environ.get(key, default)


class VcapSecretsProvider:
    """Reads secrets from bound Cloud Foundry services (VCAP_SERVICES).

    Looks up each requested key across the ``credentials`` dicts of all bound service instances.
    Falls back to plain environment variables so shared keys (e.g. OPENLINEAGE_API_KEY) still work.
    """

    def __init__(self) -> None:
        self._creds: dict[str, str] = {}
        try:
            from cfenv import AppEnv  # imported lazily; only needed in CF
        except Exception:  # pragma: no cover - cfenv always present via requirements
            return
        env = AppEnv()
        for service in getattr(env, "services", []):
            creds = getattr(service, "credentials", None) or {}
            for k, v in creds.items():
                if isinstance(v, str | int | float | bool):
                    self._creds.setdefault(k, str(v))

    def get(self, key: str, default: str | None = None) -> str | None:
        if key in self._creds:
            return self._creds[key]
        return os.environ.get(key, default)


def get_secrets_provider() -> SecretsProvider:
    """Return the appropriate provider for the current runtime."""
    if os.environ.get("VCAP_APPLICATION"):
        return VcapSecretsProvider()
    return EnvSecretsProvider()
