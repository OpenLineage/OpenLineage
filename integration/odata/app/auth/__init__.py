# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Pluggable HTTP authentication for the Datasphere client.

Every provider is an ``httpx.Auth`` so the client code is auth-agnostic. Credentials are pulled from
the :class:`app.secrets.SecretsProvider` by well-known key names (see ``config/.env.example``), never
from the (committed) YAML config.

Supported ``datasphere.auth.type`` values:
  * ``oauth2_client_credentials``  - OAuth2 client-credentials, technical user (production, default)
  * ``oauth2_refresh_token``       - OAuth2 refresh token from a one-time authorization_code login
                                     (``python -m app.oauth_login``), acts as a named user (production)
  * ``bearer``                     - static bearer token
  * ``basic``                      - HTTP Basic (generic OData servers)
  * ``cookie``                     - browser session cookie, for dev/demo only: it expires with the
                                     session and has to be copied from the browser by hand
"""

from __future__ import annotations

import logging

import httpx

from app.auth._https import require_https
from app.auth.bearer import BearerAuth
from app.auth.cookie import CookieAuth
from app.auth.oauth2_client_credentials import OAuth2ClientCredentialsAuth
from app.auth.oauth2_refresh_token import OAuth2RefreshTokenAuth

log = logging.getLogger(__name__)


class _HttpsBasicAuth(httpx.BasicAuth):
    def auth_flow(self, request: httpx.Request):
        require_https(request.url)
        yield from super().auth_flow(request)


def _require(secrets, key: str) -> str:
    value = secrets.get(key)
    if not value:
        raise ValueError(f"Missing required secret {key!r} for the configured auth type.")
    return value


def build_auth(auth_config, secrets) -> httpx.Auth:
    kind = auth_config.type
    if kind == "cookie":
        log.warning("datasphere.auth.type 'cookie' is meant for dev/demo runs; use OAuth in production.")
        return CookieAuth(_require(secrets, "DATASPHERE_COOKIE"))
    if kind == "basic":
        return _HttpsBasicAuth(
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
    if kind == "oauth2_refresh_token":
        return OAuth2RefreshTokenAuth(
            token_url=_require(secrets, "DATASPHERE_TOKEN_URL"),
            client_id=_require(secrets, "DATASPHERE_CLIENT_ID"),
            client_secret=_require(secrets, "DATASPHERE_CLIENT_SECRET"),
            refresh_token=_require(secrets, "DATASPHERE_REFRESH_TOKEN"),
            scope=auth_config.extra.get("scope"),
        )
    raise ValueError(f"Unknown datasphere.auth.type: {kind!r}")
