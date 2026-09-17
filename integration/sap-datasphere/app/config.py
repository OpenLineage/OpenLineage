# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Layered configuration.

Precedence (lowest to highest):
  1. Committed non-secret YAML defaults (``config/config.yaml`` or ``config/config.example.yaml``).
  2. Environment overrides using the ``DS_OL__<section>__<key>`` convention (12-factor; nested via
     double underscores, values parsed as YAML scalars).
Secrets are NOT part of this object -- they are resolved separately via :mod:`app.secrets` at the
point where an auth provider or transport is built, so the same code works locally and in CF.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class AuthConfig(BaseModel):
    type: str = "cookie"  # cookie | basic | bearer | oauth2_client_credentials
    # Optional non-secret knobs (e.g. oauth scope). Secret material comes from SecretsProvider.
    extra: dict[str, Any] = Field(default_factory=dict)


class DatasphereConfig(BaseModel):
    base_url: str
    tenant_host: str
    # OData service root under base_url. Default is the /dwaas-core service the web UI uses (pair with
    # cookie auth); set "/api/v1/datasphere/consumption" for the official OAuth API. Auth is chosen
    # independently via ``auth.type`` -- the base path and the auth method are orthogonal.
    odata_base_path: str = "/dwaas-core/odata/v4"
    request_timeout_seconds: int = 180
    auth: AuthConfig = Field(default_factory=AuthConfig)


class SchedulerConfig(BaseModel):
    cron: str = "0 * * * *"
    timezone: str = "UTC"


class SpacesConfig(BaseModel):
    include: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)


class LastUpdatedConfig(BaseModel):
    # ``design_time`` alone is inert on the OData backend (the catalog exposes no deployment/
    # modification dates), so default to the column proxy first, then design_time as a fallback.
    strategies: list[str] = Field(default_factory=lambda: ["max_timestamp_column", "design_time"])
    timestamp_column_patterns: list[str] = Field(default_factory=list)


class OpenLineageConfig(BaseModel):
    transport: dict[str, Any] = Field(default_factory=lambda: {"type": "console"})
    producer: str = "https://github.com/OpenLineage/OpenLineage"


class LoggingConfig(BaseModel):
    level: str = "INFO"


class Settings(BaseModel):
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    datasphere: DatasphereConfig
    spaces: SpacesConfig = Field(default_factory=SpacesConfig)
    lastupdated: LastUpdatedConfig = Field(default_factory=LastUpdatedConfig)
    openlineage: OpenLineageConfig = Field(default_factory=OpenLineageConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


_ENV_PREFIX = "DS_OL__"


def _default_config_path() -> Path:
    explicit = os.environ.get("DS_OL_CONFIG")
    if explicit:
        return Path(explicit)
    for candidate in (Path("config/config.yaml"), Path("config/config.example.yaml")):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "No config file found. Set DS_OL_CONFIG or create config/config.yaml (copy from config/config.example.yaml)."
    )


def _set_nested(target: dict[str, Any], path: list[str], value: Any) -> None:
    cursor = target
    for part in path[:-1]:
        nxt = cursor.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cursor[part] = nxt
        cursor = nxt
    cursor[path[-1]] = value


def _apply_env_overrides(data: dict[str, Any]) -> dict[str, Any]:
    for env_key, raw in os.environ.items():
        if not env_key.startswith(_ENV_PREFIX):
            continue
        path = [p.lower() for p in env_key[len(_ENV_PREFIX) :].split("__") if p]
        if not path:
            continue
        # Parse scalar as YAML so ints/bools/lists Just Work; fall back to raw string.
        try:
            value = yaml.safe_load(raw)
        except yaml.YAMLError:
            value = raw
        _set_nested(data, path, value)
    return data


def load_settings(path: str | os.PathLike[str] | None = None) -> Settings:
    cfg_path = Path(path) if path else _default_config_path()
    data = yaml.safe_load(cfg_path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping, got {type(data).__name__}")
    data = _apply_env_overrides(data)
    return Settings.model_validate(data)
