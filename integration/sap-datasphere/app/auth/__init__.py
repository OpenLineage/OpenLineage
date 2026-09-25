# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Pluggable HTTP authentication for the Datasphere client.

Every provider is an ``httpx.Auth`` so the client code is auth-agnostic. Credentials are pulled from
the :class:`app.secrets.SecretsProvider` by well-known key names (see ``config/.env.example``), never
from the (committed) YAML config.

Supported ``datasphere.auth.type`` values:
  * ``cookie``                     - browser session cookie (works today against the trial tenant)
  * ``basic``                      - HTTP Basic
  * ``bearer``                     - static bearer token
  * ``oauth2_client_credentials``  - OAuth2 client-credentials with token caching (production)
"""

from __future__ import annotations

import httpx

from app.auth.bearer import BearerAuth
from app.auth.cookie import CookieAuth
from app.auth.oauth2_client_credentials import OAuth2ClientCredentialsAuth


def _require(secrets, key: str) -> str:
    value = secrets.get(key)
    if not value:
        raise ValueError(f"Missing required secret {key!r} for the configured auth type.")
    return value


def build_auth(auth_config, secrets) -> httpx.Auth:
    kind = auth_config.type
    if kind == "cookie":
        return CookieAuth(_require(secrets, "DATASPHERE_COOKIE"))
    if kind == "basic":
        return httpx.BasicAuth(
            _require(secrets, "DATASPHERE_USERNAME"),
            _require(secrets, "DATASPHERE_PASSWORD"),
        )
    if kind == "bearer":
        return BearerAuth(_require(secrets, "DATASPHERE_TOKEN"))
    if kind == "oauth2_client_credentials":
        return OAuth2ClientCredentialsAuth(
            token_url=_require(secrets, "DATASPHERE_TOKEN_URL"),
            client_id=_require(secrets, "DATASPHERE_CLIENT_ID"),
            client_secret=_require(secrets, "DATASPHERE_CLIENT_SECRET"),
            scope=auth_config.extra.get("scope"),
        )
    raise ValueError(f"Unknown datasphere.auth.type: {kind!r}")
