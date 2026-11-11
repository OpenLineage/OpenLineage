# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""One-time OAuth2 authorization_code login that prints a refresh token.

SAP Datasphere documents the 3-legged authorization_code flow for its consumption APIs (OAuth client
with purpose "Interactive Usage"). A scheduled job cannot open a browser, so run this once:

    python -m app.oauth_login [--redirect-uri http://localhost:8080/callback]

It reads ``DATASPHERE_TOKEN_URL``, ``DATASPHERE_CLIENT_ID`` and ``DATASPHERE_CLIENT_SECRET`` from the
usual secrets sources, prints the authorization URL to open in a browser, waits for the redirect on a
local port, exchanges the code and prints the refresh token. Store it as ``DATASPHERE_REFRESH_TOKEN``
and set ``datasphere.auth.type: oauth2_refresh_token``. The redirect URI must be registered on the
OAuth client exactly as passed here.
"""

from __future__ import annotations

import argparse
import secrets as _secrets
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from app.auth._https import require_https
from app.secrets import get_secrets_provider

_DEFAULT_REDIRECT_URI = "http://localhost:8080/callback"


def authorize_url(token_url: str, client_id: str, redirect_uri: str, state: str) -> str:
    """XSUAA serves ``/oauth/authorize`` next to ``/oauth/token``."""
    require_https(token_url)
    base = token_url.rstrip("/")
    if not base.endswith("/token"):
        raise ValueError(f"cannot derive the authorize URL from token URL {token_url!r} (expected .../oauth/token)")
    query = urlencode({"response_type": "code", "client_id": client_id, "redirect_uri": redirect_uri, "state": state})
    return f"{base[: -len('/token')]}/authorize?{query}"


def exchange_code(token_url: str, client_id: str, client_secret: str, code: str, redirect_uri: str) -> str:
    """Exchange an authorization code for a refresh token."""
    require_https(token_url)
    resp = httpx.post(
        token_url,
        data={"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri},
        auth=(client_id, client_secret),
        headers={"Accept": "application/json"},
        timeout=30.0,
    )
    if resp.status_code != 200:
        # The body is left out: error responses can echo request parameters.
        raise RuntimeError(f"token endpoint returned HTTP {resp.status_code}")
    try:
        refresh_token = resp.json()["refresh_token"]
    except (ValueError, KeyError, TypeError) as exc:
        raise RuntimeError(
            "token response has no refresh_token; check that the OAuth client allows the refresh_token grant"
        ) from exc
    if not isinstance(refresh_token, str) or not refresh_token:
        raise RuntimeError("token response has an empty refresh_token")
    return refresh_token


def _wait_for_code(redirect_uri: str, state: str) -> str:
    target = urlparse(redirect_uri)
    if target.scheme != "http" or target.hostname not in ("localhost", "127.0.0.1"):
        raise ValueError("--redirect-uri must be an http://localhost or http://127.0.0.1 URL")
    result: dict[str, str] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (http.server API)
            url = urlparse(self.path)
            params = {k: v[0] for k, v in parse_qs(url.query).items()}
            if url.path != (target.path or "/"):
                self.send_error(404)
                return
            if params.get("state") != state:
                result["error"] = "state mismatch"
            elif "code" in params:
                result["code"] = params["code"]
            else:
                result["error"] = params.get("error_description") or params.get("error") or "no code in redirect"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Login finished. You can close this tab and return to the terminal.\n")

        def log_message(self, *args) -> None:  # keep the code out of the terminal log
            pass

    with HTTPServer((target.hostname, target.port or 80), Handler) as server:
        while not result:
            server.handle_request()
    if "error" in result:
        raise RuntimeError(f"authorization failed: {result['error']}")
    return result["code"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Get an OAuth2 refresh token for oauth2_refresh_token auth.")
    parser.add_argument("--redirect-uri", default=_DEFAULT_REDIRECT_URI, help=f"default: {_DEFAULT_REDIRECT_URI}")
    args = parser.parse_args(argv)

    provider = get_secrets_provider()
    values = {k: provider.get(k) for k in ("DATASPHERE_TOKEN_URL", "DATASPHERE_CLIENT_ID", "DATASPHERE_CLIENT_SECRET")}
    missing = [k for k, v in values.items() if not v]
    if missing:
        print(f"Missing secrets: {', '.join(missing)} (set them in .env or the environment).", file=sys.stderr)
        return 1
    token_url, client_id, client_secret = values.values()

    state = _secrets.token_urlsafe(16)
    print("Open this URL in a browser and log in:\n")
    print(f"  {authorize_url(token_url, client_id, args.redirect_uri, state)}\n")
    print(f"Waiting for the redirect to {args.redirect_uri} ...", file=sys.stderr)
    try:
        code = _wait_for_code(args.redirect_uri, state)
        refresh_token = exchange_code(token_url, client_id, client_secret, code, args.redirect_uri)
    except (RuntimeError, ValueError, OSError, httpx.HTTPError) as exc:
        print(f"Login failed: {exc}", file=sys.stderr)
        return 1
    print("\nStore this as DATASPHERE_REFRESH_TOKEN and set datasphere.auth.type: oauth2_refresh_token:\n")
    print(refresh_token)
    return 0


if __name__ == "__main__":
    sys.exit(main())
