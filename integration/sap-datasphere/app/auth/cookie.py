# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Cookie authentication (browser session cookie)."""

from __future__ import annotations

from collections.abc import Generator

import httpx


class CookieAuth(httpx.Auth):
    """Sends a raw ``Cookie`` header (e.g. ``JSESSIONID=...``) on every request."""

    def __init__(self, cookie: str) -> None:
        self._cookie = cookie

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Cookie"] = self._cookie
        yield request
