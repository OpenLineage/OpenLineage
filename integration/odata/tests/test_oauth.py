# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.auth import build_auth
from app.auth.bearer import BearerAuth
from app.auth.cookie import CookieAuth
from app.auth.oauth2_client_credentials import OAuth2ClientCredentialsAuth
from app.auth.oauth2_refresh_token import OAuth2RefreshTokenAuth
from app.config import AuthConfig

TOKEN_URL = "https://tenant.authentication.eu10.hana.ondemand.com/oauth/token"
OAUTH_SECRETS = {
    "DATASPHERE_TOKEN_URL": TOKEN_URL,
    "DATASPHERE_CLIENT_ID": "client-id",
    "DATASPHERE_CLIENT_SECRET": "client-s3cret",
    "DATASPHERE_REFRESH_TOKEN": "refresh-1",
}


class _TokenEndpoint:
    """Fake ``httpx.post`` recording token requests and handing out numbered access tokens."""

    def __init__(self, expires_in=3600, extra=None):
        self.requests: list[dict] = []
        self.expires_in = expires_in
        self.extra = extra or {}

    def __call__(self, url, data=None, auth=None, **kwargs):
        self.requests.append({"url": url, "data": dict(data), "auth": auth})
        payload = {"access_token": f"token-{len(self.requests)}", "expires_in": self.expires_in, **self.extra}
        return httpx.Response(200, request=httpx.Request("POST", url), json=payload)


def _client(auth, handler=None):
    transport = httpx.MockTransport(handler or (lambda request: httpx.Response(200)))
    return httpx.Client(auth=auth, transport=transport)


# ------------------------------------------------------------------ build_auth


@pytest.mark.parametrize(
    "kind, secrets, expected",
    [
        ("oauth2_client_credentials", OAUTH_SECRETS, OAuth2ClientCredentialsAuth),
        ("oauth2_refresh_token", OAUTH_SECRETS, OAuth2RefreshTokenAuth),
        ("bearer", {"DATASPHERE_TOKEN": "t"}, BearerAuth),
        ("cookie", {"DATASPHERE_COOKIE": "JSESSIONID=abc"}, CookieAuth),
        ("basic", {"DATASPHERE_USERNAME": "u", "DATASPHERE_PASSWORD": "p"}, httpx.BasicAuth),
    ],
)
def test_build_auth_returns_provider_per_type(kind, secrets, expected):
    assert isinstance(build_auth(AuthConfig(type=kind), secrets), expected)


def test_build_auth_names_the_missing_secret():
    secrets = {k: v for k, v in OAUTH_SECRETS.items() if k != "DATASPHERE_REFRESH_TOKEN"}
    with pytest.raises(ValueError, match="DATASPHERE_REFRESH_TOKEN"):
        build_auth(AuthConfig(type="oauth2_refresh_token"), secrets)


def test_unknown_auth_type_is_rejected_by_config():
    with pytest.raises(ValueError):
        AuthConfig(type="api_key")


def test_cookie_auth_logs_dev_only_warning(caplog):
    build_auth(AuthConfig(type="cookie"), {"DATASPHERE_COOKIE": "JSESSIONID=abc"})
    assert "dev/demo" in caplog.text


# ------------------------------------------------------------------ client credentials


def test_client_credentials_token_is_fetched_once_and_cached(monkeypatch):
    endpoint = _TokenEndpoint()
    monkeypatch.setattr(httpx, "post", endpoint)
    seen = []
    auth = OAuth2ClientCredentialsAuth(TOKEN_URL, "client-id", "client-s3cret", scope="uaa.resource")
    client = _client(auth, lambda request: seen.append(request.headers["Authorization"]) or httpx.Response(200))

    client.get("https://h/a")
    client.get("https://h/b")

    assert seen == ["Bearer token-1", "Bearer token-1"]
    assert len(endpoint.requests) == 1
    assert endpoint.requests[0]["data"] == {"grant_type": "client_credentials", "scope": "uaa.resource"}
    assert endpoint.requests[0]["auth"] == ("client-id", "client-s3cret")


def test_token_is_refreshed_near_expiry(monkeypatch):
    # expires_in below the 60s skew: the cached token is already stale on the next request.
    endpoint = _TokenEndpoint(expires_in=30)
    monkeypatch.setattr(httpx, "post", endpoint)
    client = _client(OAuth2ClientCredentialsAuth(TOKEN_URL, "client-id", "client-s3cret"))

    client.get("https://h/a")
    client.get("https://h/b")

    assert len(endpoint.requests) == 2


def test_401_forces_one_refresh_and_retry(monkeypatch):
    endpoint = _TokenEndpoint()
    monkeypatch.setattr(httpx, "post", endpoint)
    seen = []

    def handler(request):
        seen.append(request.headers["Authorization"])
        return httpx.Response(401 if len(seen) == 1 else 200)

    resp = _client(OAuth2ClientCredentialsAuth(TOKEN_URL, "client-id", "client-s3cret"), handler).get("https://h/a")

    assert resp.status_code == 200
    assert seen == ["Bearer token-1", "Bearer token-2"]


# ------------------------------------------------------------------ refresh token


def test_refresh_token_grant_body(monkeypatch):
    endpoint = _TokenEndpoint()
    monkeypatch.setattr(httpx, "post", endpoint)
    _client(OAuth2RefreshTokenAuth(TOKEN_URL, "client-id", "client-s3cret", "refresh-1")).get("https://h/a")

    assert endpoint.requests[0]["data"] == {"grant_type": "refresh_token", "refresh_token": "refresh-1"}
    assert endpoint.requests[0]["auth"] == ("client-id", "client-s3cret")


def test_rotated_refresh_token_is_used_for_the_next_refresh(monkeypatch, caplog):
    endpoint = _TokenEndpoint(expires_in=30, extra={"refresh_token": "refresh-2"})
    monkeypatch.setattr(httpx, "post", endpoint)
    client = _client(OAuth2RefreshTokenAuth(TOKEN_URL, "client-id", "client-s3cret", "refresh-1"))

    client.get("https://h/a")
    client.get("https://h/b")

    assert [r["data"]["refresh_token"] for r in endpoint.requests] == ["refresh-1", "refresh-2"]
    assert "rotated" in caplog.text
    assert "refresh-2" not in caplog.text  # the token value is never logged


def test_refresh_token_requires_https_token_url():
    with pytest.raises(ValueError, match="non-HTTPS"):
        OAuth2RefreshTokenAuth("http://idp.test/oauth/token", "c", "s", "r")


# ------------------------------------------------------------------ oauth_login helper


def test_authorize_url_is_derived_from_token_url():
    from app.oauth_login import authorize_url

    url = urlparse(authorize_url(TOKEN_URL, "client-id", "http://localhost:8080/callback", "st"))
    assert f"{url.scheme}://{url.netloc}{url.path}" == TOKEN_URL.replace("/token", "/authorize")
    assert parse_qs(url.query) == {
        "response_type": ["code"],
        "client_id": ["client-id"],
        "redirect_uri": ["http://localhost:8080/callback"],
        "state": ["st"],
    }
    with pytest.raises(ValueError, match="authorize URL"):
        authorize_url("https://idp.test/other", "c", "http://localhost:8080/callback", "st")


def test_exchange_code_returns_refresh_token(monkeypatch):
    from app.oauth_login import exchange_code

    endpoint = _TokenEndpoint(extra={"refresh_token": "refresh-new"})
    monkeypatch.setattr(httpx, "post", endpoint)

    token = exchange_code(TOKEN_URL, "client-id", "client-s3cret", "the-code", "http://localhost:8080/callback")

    assert token == "refresh-new"
    assert endpoint.requests[0]["data"] == {
        "grant_type": "authorization_code",
        "code": "the-code",
        "redirect_uri": "http://localhost:8080/callback",
    }


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(400, json={"error": "invalid_grant", "code": "the-code"}),
        httpx.Response(200, json={"access_token": "a"}),  # no refresh_token issued
    ],
)
def test_exchange_code_errors_do_not_leak_the_body(monkeypatch, response):
    from app.oauth_login import exchange_code

    monkeypatch.setattr(httpx, "post", lambda url, **kw: response)
    with pytest.raises(RuntimeError) as excinfo:
        exchange_code(TOKEN_URL, "client-id", "client-s3cret", "the-code", "http://localhost:8080/callback")
    assert "the-code" not in str(excinfo.value)
