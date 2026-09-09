# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import HttpConfig, OAuth2ClientCredentialsTokenProvider

TOKEN_ENDPOINT = "https://auth.example.com/token"
CONFIG = {"clientId": "test-client-id", "clientSecret": "test-client-secret", "tokenEndpoint": TOKEN_ENDPOINT}


def _token_response(**payload):
    response = Mock()
    response.status_code = 200
    response.json.return_value = payload or {"access_token": "access-token-value", "expires_in": 600}
    return response


class TestOAuth2ClientCredentialsTokenProvider:
    """Tests for OAuth2ClientCredentialsTokenProvider"""

    @pytest.mark.parametrize("missing_key", ["clientId", "clientSecret", "tokenEndpoint"])
    def test_requires_client_id_client_secret_and_token_endpoint(self, missing_key):
        """Test that clientId, clientSecret and tokenEndpoint are required"""
        config = {key: value for key, value in CONFIG.items() if key != missing_key}

        with pytest.raises(KeyError, match=f"{missing_key} is required"):
            OAuth2ClientCredentialsTokenProvider(config)

    def test_initialization_with_defaults(self):
        """Test OAuth2ClientCredentialsTokenProvider initialization with default values"""
        provider = OAuth2ClientCredentialsTokenProvider(CONFIG)

        assert provider.client_id == "test-client-id"
        assert provider.client_secret == "test-client-secret"
        assert provider.token_endpoint == TOKEN_ENDPOINT
        assert provider.scope is None
        assert provider.client_auth_method == "client_secret_basic"
        assert provider.token_fields == ["access_token"]
        assert provider.expires_in_field == "expires_in"
        assert provider.token_refresh_buffer == 120

    def test_snake_case_config(self):
        """Test OAuth2ClientCredentialsTokenProvider accepts snake_case config keys"""
        provider = OAuth2ClientCredentialsTokenProvider(
            {
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "token_endpoint": TOKEN_ENDPOINT,
                "client_auth_method": "client_secret_post",
                "token_refresh_buffer": "30",
            }
        )

        assert provider.client_id == "test-client-id"
        assert provider.client_secret == "test-client-secret"
        assert provider.token_endpoint == TOKEN_ENDPOINT
        assert provider.client_auth_method == "client_secret_post"
        assert provider.token_refresh_buffer == 30

    def test_rejects_unknown_client_auth_method(self):
        """Test that an unsupported clientAuthMethod raises error"""
        with pytest.raises(ValueError, match="clientAuthMethod must be one of"):
            OAuth2ClientCredentialsTokenProvider({**CONFIG, "clientAuthMethod": "private_key_jwt"})

    @patch("requests.post")
    def test_get_bearer_sends_client_credentials_in_authorization_header(self, mock_post):
        """Test client_secret_basic sends credentials as HTTP basic auth"""
        mock_post.return_value = _token_response()
        provider = OAuth2ClientCredentialsTokenProvider(CONFIG)

        assert provider.get_bearer() == "Bearer access-token-value"

        call_args = mock_post.call_args
        assert call_args.args[0] == TOKEN_ENDPOINT
        assert call_args.kwargs["data"] == {"grant_type": "client_credentials"}
        assert call_args.kwargs["auth"] == ("test-client-id", "test-client-secret")
        assert call_args.kwargs["headers"] == {"Content-Type": "application/x-www-form-urlencoded"}
        assert call_args.kwargs["timeout"] == 10

    @patch("requests.post")
    def test_get_bearer_sends_client_credentials_in_body(self, mock_post):
        """Test client_secret_post sends credentials and scope in the request body"""
        mock_post.return_value = _token_response()
        provider = OAuth2ClientCredentialsTokenProvider(
            {**CONFIG, "clientAuthMethod": "client_secret_post", "scope": "openid"}
        )

        assert provider.get_bearer() == "Bearer access-token-value"

        call_args = mock_post.call_args
        assert call_args.kwargs["data"] == {
            "grant_type": "client_credentials",
            "scope": "openid",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }
        assert call_args.kwargs["auth"] is None

    @patch("requests.post")
    def test_get_bearer_caches_token(self, mock_post):
        """Test that token is cached and reused"""
        mock_post.return_value = _token_response()
        provider = OAuth2ClientCredentialsTokenProvider(CONFIG)

        assert provider.get_bearer() == "Bearer access-token-value"
        assert provider.get_bearer() == "Bearer access-token-value"
        assert mock_post.call_count == 1

    @patch("requests.post")
    def test_get_bearer_refreshes_token_before_expiry(self, mock_post):
        """Test that token is refreshed tokenRefreshBuffer seconds before expiry"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = [
            {"access_token": "token-1", "expires_in": 600},
            {"access_token": "token-2", "expires_in": 600},
        ]
        mock_post.return_value = mock_response
        provider = OAuth2ClientCredentialsTokenProvider(CONFIG)

        initial_time = 1000000.0
        with patch.object(provider, "_get_current_time", return_value=initial_time):
            assert provider.get_bearer() == "Bearer token-1"

        # Token expires at 600, default buffer is 120: still cached at 479, refreshed at 480
        with patch.object(provider, "_get_current_time", return_value=initial_time + 479):
            assert provider.get_bearer() == "Bearer token-1"
        assert mock_post.call_count == 1

        with patch.object(provider, "_get_current_time", return_value=initial_time + 480):
            assert provider.get_bearer() == "Bearer token-2"
        assert mock_post.call_count == 2

    @patch("requests.post")
    def test_get_bearer_extracts_expiry_from_jwt(self, mock_post):
        """Test expiry extraction from JWT payload when expires_in is not in response"""
        encoded_payload = base64.urlsafe_b64encode(b'{"exp": 2000000}').decode().rstrip("=")
        jwt_token = f"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.{encoded_payload}.signature"
        mock_post.return_value = _token_response(access_token=jwt_token)
        provider = OAuth2ClientCredentialsTokenProvider(CONFIG)

        assert provider.get_bearer() == f"Bearer {jwt_token}"
        assert provider._token_expiry == 2000000.0

    @patch("requests.post")
    def test_get_bearer_raises_on_http_error(self, mock_post):
        """Test that HTTP errors are raised"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = Exception("Unauthorized")
        mock_post.return_value = mock_response
        provider = OAuth2ClientCredentialsTokenProvider(CONFIG)

        with pytest.raises(RuntimeError, match="Failed to fetch OAuth2 access token"):
            provider.get_bearer()

    @patch("requests.post")
    def test_get_bearer_raises_on_missing_access_token(self, mock_post):
        """Test that a response without access_token raises error"""
        mock_post.return_value = _token_response(token_type="Bearer")
        provider = OAuth2ClientCredentialsTokenProvider(CONFIG)

        with pytest.raises(RuntimeError, match="Failed to fetch OAuth2 access token"):
            provider.get_bearer()

    @patch("requests.post")
    def test_get_bearer_fetches_token_once_for_concurrent_calls(self, mock_post):
        """Test that concurrent callers share a single token request"""

        def slow_post(*args, **kwargs):
            time.sleep(0.05)
            return _token_response()

        mock_post.side_effect = slow_post
        provider = OAuth2ClientCredentialsTokenProvider(CONFIG)

        with ThreadPoolExecutor(max_workers=8) as executor:
            bearers = list(executor.map(lambda _: provider.get_bearer(), range(8)))

        assert bearers == ["Bearer access-token-value"] * 8
        assert mock_post.call_count == 1


class TestHttpConfigWithOAuth2ClientCredentialsAuth:
    """Test HttpConfig integration with OAuth2ClientCredentialsTokenProvider"""

    def test_http_config_loads_oauth2_client_credentials_auth(self):
        """Test that HttpConfig can load oauth2_client_credentials auth configuration"""
        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "auth": {"type": "oauth2_client_credentials", **CONFIG},
            }
        )

        assert isinstance(config.auth, OAuth2ClientCredentialsTokenProvider)
        assert config.auth.client_id == "test-client-id"
        assert config.auth.client_secret == "test-client-secret"
        assert config.auth.token_endpoint == TOKEN_ENDPOINT

    @patch.dict(
        "os.environ",
        {
            "OPENLINEAGE__TRANSPORT__TYPE": "http",
            "OPENLINEAGE__TRANSPORT__URL": "http://backend:5000",
            "OPENLINEAGE__TRANSPORT__AUTH__TYPE": "oauth2_client_credentials",
            "OPENLINEAGE__TRANSPORT__AUTH__CLIENT_ID": "env-client-id",
            "OPENLINEAGE__TRANSPORT__AUTH__CLIENT_SECRET": "env-client-secret",
            "OPENLINEAGE__TRANSPORT__AUTH__TOKEN_ENDPOINT": TOKEN_ENDPOINT,
            "OPENLINEAGE__TRANSPORT__AUTH__CLIENT_AUTH_METHOD": "client_secret_post",
            "OPENLINEAGE__TRANSPORT__AUTH__SCOPE": "openid",
            "OPENLINEAGE__TRANSPORT__AUTH__TOKEN_REFRESH_BUFFER": "180",
        },
        clear=True,
    )
    def test_load_oauth2_client_credentials_auth_from_environment_variables(self):
        client = OpenLineageClient()
        auth = client.transport.config.auth

        assert isinstance(auth, OAuth2ClientCredentialsTokenProvider)
        assert auth.client_id == "env-client-id"
        assert auth.client_secret == "env-client-secret"
        assert auth.token_endpoint == TOKEN_ENDPOINT
        assert auth.client_auth_method == "client_secret_post"
        assert auth.scope == "openid"
        assert auth.token_refresh_buffer == 180

    @patch.dict(
        "os.environ",
        {
            "OPENLINEAGE__TRANSPORT__TYPE": "http",
            "OPENLINEAGE__TRANSPORT__URL": "http://backend:5000",
            "OPENLINEAGE__TRANSPORT__AUTH__TYPE": "oauth2_client_credentials",
            "OPENLINEAGE__TRANSPORT__AUTH__CLIENTID": "env-client-id",
            "OPENLINEAGE__TRANSPORT__AUTH__CLIENTSECRET": "env-client-secret",
            "OPENLINEAGE__TRANSPORT__AUTH__TOKENENDPOINT": TOKEN_ENDPOINT,
        },
        clear=True,
    )
    def test_load_oauth2_client_credentials_auth_from_invalid_environment_variables_fails(self):
        with pytest.raises(KeyError, match="clientId is required"):
            OpenLineageClient()
