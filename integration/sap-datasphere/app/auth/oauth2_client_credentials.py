# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""OAuth2 client-credentials authentication with in-memory token caching.

Used for the official SAP Datasphere consumption API in production (technical user / 2-legged).
Client-credentials issues no refresh token, so we simply re-POST the token endpoint when the cached
token nears expiry or a request comes back 401.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Generator

import httpx

# Refresh this many seconds before the token actually expires, to avoid edge-of-expiry 401s.
_EXPIRY_SKEW_SECONDS = 60


class OAuth2ClientCredentialsAuth(httpx.Auth):
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
        token_request_timeout: float = 30.0,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._timeout = token_request_timeout
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    def _fetch_token(self) -> None:
        data = {"grant_type": "client_credentials"}
        if self._scope:
            data["scope"] = self._scope
        resp = httpx.post(
            self._token_url,
            data=data,
            auth=(self._client_id, self._client_secret),
            headers={"Accept": "application/json"},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._expires_at = time.monotonic() + max(0, expires_in - _EXPIRY_SKEW_SECONDS)

    def _valid_token(self) -> str:
        with self._lock:
            if not self._token or time.monotonic() >= self._expires_at:
                self._fetch_token()
            assert self._token is not None
            return self._token

    def _force_refresh(self) -> str:
        with self._lock:
            self._fetch_token()
            assert self._token is not None
            return self._token

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self._valid_token()}"
        response = yield request
        if response.status_code == 401:
            request.headers["Authorization"] = f"Bearer {self._force_refresh()}"
            yield request
