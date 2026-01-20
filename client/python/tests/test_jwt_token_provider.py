# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import json
import time
from unittest.mock import Mock, patch

import pytest
from openlineage.client.transport.http import HttpConfig, JwtTokenProvider


class TestJwtTokenProvider:
    """Tests for JwtTokenProvider"""

    def test_jwt_token_provider_requires_api_key(self):
        """Test that JwtTokenProvider requires apiKey"""
        with pytest.raises(KeyError, match="apiKey is required"):
            JwtTokenProvider({"tokenEndpoint": "https://auth.example.com/token"})

    def test_jwt_token_provider_requires_token_endpoint(self):
        """Test that JwtTokenProvider requires tokenEndpoint"""
        with pytest.raises(KeyError, match="tokenEndpoint is required"):
            JwtTokenProvider({"apiKey": "test-key"})

    def test_jwt_token_provider_initialization_with_defaults(self):
        """Test JwtTokenProvider initialization with default values"""
        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        assert provider.api_key == "test-key"
        assert provider.token_endpoint == "https://auth.example.com/token"
        assert provider.token_fields == ["token", "access_token"]
        assert provider.expires_in_field == "expires_in"
        assert provider.grant_type == "urn:ietf:params:oauth:grant-type:jwt-bearer"
        assert provider.response_type == "token"

    def test_jwt_token_provider_initialization_with_custom_values(self):
        """Test JwtTokenProvider initialization with custom values"""
        provider = JwtTokenProvider(
            {
                "apiKey": "test-key",
                "tokenEndpoint": "https://auth.example.com/token",
                "tokenFields": ["custom_token", "access_token"],
                "expiresInField": "custom_expires",
                "grantType": "custom_grant",
                "responseType": "custom_response",
            }
        )

        assert provider.token_fields == ["custom_token", "access_token"]
        assert provider.expires_in_field == "custom_expires"
        assert provider.grant_type == "custom_grant"
        assert provider.response_type == "custom_response"

    def test_jwt_token_provider_snake_case_config(self):
        """Test JwtTokenProvider accepts snake_case config keys"""
        provider = JwtTokenProvider(
            {
                "api_key": "test-key",
                "token_endpoint": "https://auth.example.com/token",
                "token_fields": ["token"],
                "expires_in_field": "expires_in",
                "grant_type": "custom_grant",
                "response_type": "custom_response",
            }
        )

        assert provider.api_key == "test-key"
        assert provider.token_endpoint == "https://auth.example.com/token"
        assert provider.token_fields == ["token"]
        assert provider.expires_in_field == "expires_in"
        assert provider.grant_type == "custom_grant"
        assert provider.response_type == "custom_response"

    @patch("requests.post")
    def test_get_bearer_fetches_token_successfully(self, mock_post):
        """Test successful token fetch"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "jwt-token-value", "expires_in": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        bearer = provider.get_bearer()

        assert bearer == "Bearer jwt-token-value"
        mock_post.assert_called_once()

        # Verify request parameters
        call_args = mock_post.call_args
        assert call_args.args[0] == "https://auth.example.com/token"
        assert call_args.kwargs["data"]["apikey"] == "test-key"
        assert call_args.kwargs["data"]["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer"
        assert call_args.kwargs["data"]["response_type"] == "token"

    @patch("requests.post")
    def test_get_bearer_caches_token(self, mock_post):
        """Test that token is cached and reused"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "jwt-token-value", "expires_in": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        # First call should fetch token
        bearer1 = provider.get_bearer()
        assert bearer1 == "Bearer jwt-token-value"

        # Second call should use cached token
        bearer2 = provider.get_bearer()
        assert bearer2 == "Bearer jwt-token-value"

        # Should only call endpoint once
        assert mock_post.call_count == 1

    @patch("requests.post")
    def test_get_bearer_refreshes_expired_token(self, mock_post):
        """Test that expired token is refreshed"""
        mock_response = Mock()
        mock_response.status_code = 200
        # First response with short expiry
        mock_response.json.side_effect = [
            {"token": "jwt-token-1", "expires_in": 1},
            {"token": "jwt-token-2", "expires_in": 3600},
        ]
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        # First call
        bearer1 = provider.get_bearer()
        assert bearer1 == "Bearer jwt-token-1"

        # Wait for token to expire (plus buffer)
        time.sleep(2)

        # Second call should fetch new token
        bearer2 = provider.get_bearer()
        assert bearer2 == "Bearer jwt-token-2"

        # Should call endpoint twice
        assert mock_post.call_count == 2

    @patch("requests.post")
    def test_get_bearer_with_access_token_field(self, mock_post):
        """Test token extraction with access_token field"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "jwt-access-token", "expires_in": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        bearer = provider.get_bearer()
        assert bearer == "Bearer jwt-access-token"

    @patch("requests.post")
    def test_get_bearer_with_custom_token_field(self, mock_post):
        """Test token extraction with custom field name"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"custom_token": "jwt-custom-token", "expires_in": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider(
            {
                "apiKey": "test-key",
                "tokenEndpoint": "https://auth.example.com/token",
                "tokenFields": ["custom_token", "token"],
            }
        )

        bearer = provider.get_bearer()
        assert bearer == "Bearer jwt-custom-token"

    @patch("requests.post")
    def test_get_bearer_tries_multiple_token_fields(self, mock_post):
        """Test that provider tries multiple token field names"""
        mock_response = Mock()
        mock_response.status_code = 200
        # Only has 'token' field, not 'custom_token'
        mock_response.json.return_value = {"token": "jwt-token-value", "expires_in": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider(
            {
                "apiKey": "test-key",
                "tokenEndpoint": "https://auth.example.com/token",
                "tokenFields": ["custom_token", "token"],
            }
        )

        bearer = provider.get_bearer()
        assert bearer == "Bearer jwt-token-value"

    @patch("requests.post")
    def test_get_bearer_case_insensitive_token_field(self, mock_post):
        """Test case-insensitive token field matching"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Token": "jwt-token-value", "expires_in": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        bearer = provider.get_bearer()
        assert bearer == "Bearer jwt-token-value"

    @patch("requests.post")
    def test_get_bearer_raises_on_missing_token_field(self, mock_post):
        """Test that missing token field raises error"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"some_other_field": "value"}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        with pytest.raises(RuntimeError, match="Failed to fetch JWT token"):
            provider.get_bearer()

    @patch("requests.post")
    def test_get_bearer_raises_on_http_error(self, mock_post):
        """Test that HTTP errors are raised"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = Exception("Unauthorized")
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        with pytest.raises(RuntimeError, match="Failed to fetch JWT token"):
            provider.get_bearer()

    @patch("requests.post")
    def test_get_bearer_extracts_expiry_from_jwt(self, mock_post):
        """Test expiry extraction from JWT payload when not in response"""
        # Create a JWT with exp claim
        expiry = int(time.time()) + 3600
        payload = json.dumps({"sub": "test", "exp": expiry})
        encoded_payload = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
        jwt_token = f"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.{encoded_payload}.signature"

        mock_response = Mock()
        mock_response.status_code = 200
        # Response without expires_in field
        mock_response.json.return_value = {"token": jwt_token}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        bearer = provider.get_bearer()
        assert bearer == f"Bearer {jwt_token}"

        # Token should be cached with expiry from JWT
        assert provider._token_expiry is not None
        assert provider._token_expiry > time.time()

    @patch("requests.post")
    def test_get_bearer_with_custom_expires_in_field(self, mock_post):
        """Test custom expires_in field name"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "jwt-token-value", "custom_expires": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider(
            {
                "apiKey": "test-key",
                "tokenEndpoint": "https://auth.example.com/token",
                "expiresInField": "custom_expires",
            }
        )

        bearer = provider.get_bearer()
        assert bearer == "Bearer jwt-token-value"
        assert provider._token_expiry is not None

    @patch("requests.post")
    def test_get_bearer_case_insensitive_expires_field(self, mock_post):
        """Test case-insensitive expires_in field matching"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "jwt-token-value", "ExpiresIn": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider({"apiKey": "test-key", "tokenEndpoint": "https://auth.example.com/token"})

        bearer = provider.get_bearer()
        assert bearer == "Bearer jwt-token-value"
        assert provider._token_expiry is not None

    @patch("requests.post")
    def test_get_bearer_with_ibm_cloud_iam_config(self, mock_post):
        """Test IBM Cloud IAM configuration"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "ibm-token", "expires_in": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider(
            {
                "apiKey": "ibm-api-key",
                "tokenEndpoint": "https://iam.cloud.ibm.com/identity/token",
                "grantType": "urn:ibm:params:oauth:grant-type:apikey",
                "responseType": "cloud_iam",
            }
        )

        bearer = provider.get_bearer()
        assert bearer == "Bearer ibm-token"

        # Verify request parameters
        call_args = mock_post.call_args
        assert call_args.kwargs["data"]["grant_type"] == "urn:ibm:params:oauth:grant-type:apikey"
        assert call_args.kwargs["data"]["response_type"] == "cloud_iam"

    @patch("requests.post")
    def test_get_bearer_with_special_characters_in_api_key(self, mock_post):
        """Test that special characters in API key are properly encoded"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "jwt-token-value", "expires_in": 3600}
        mock_post.return_value = mock_response

        provider = JwtTokenProvider(
            {
                "apiKey": "test+key=with&special?chars",
                "tokenEndpoint": "https://auth.example.com/token",
            }
        )

        bearer = provider.get_bearer()
        assert bearer == "Bearer jwt-token-value"

        # Verify API key is in the request
        call_args = mock_post.call_args
        assert call_args.kwargs["data"]["apikey"] == "test+key=with&special?chars"


class TestHttpConfigWithJwtAuth:
    """Test HttpConfig integration with JwtTokenProvider"""

    def test_http_config_loads_jwt_auth(self):
        """Test that HttpConfig can load JWT auth configuration"""
        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "auth": {
                    "type": "jwt",
                    "apiKey": "test-api-key",
                    "tokenEndpoint": "https://auth.example.com/token",
                },
            }
        )

        assert isinstance(config.auth, JwtTokenProvider)
        assert config.auth.api_key == "test-api-key"
        assert config.auth.token_endpoint == "https://auth.example.com/token"

    def test_http_config_loads_jwt_auth_with_custom_fields(self):
        """Test HttpConfig with custom JWT configuration"""
        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "auth": {
                    "type": "jwt",
                    "apiKey": "test-api-key",
                    "tokenEndpoint": "https://auth.example.com/token",
                    "tokenFields": ["custom_token", "access_token"],
                    "expiresInField": "custom_expires",
                    "grantType": "custom_grant",
                    "responseType": "custom_response",
                },
            }
        )

        assert isinstance(config.auth, JwtTokenProvider)
        assert config.auth.token_fields == ["custom_token", "access_token"]
        assert config.auth.expires_in_field == "custom_expires"
        assert config.auth.grant_type == "custom_grant"
        assert config.auth.response_type == "custom_response"

    def test_http_config_loads_jwt_auth_ibm_cloud(self):
        """Test HttpConfig with IBM Cloud IAM configuration"""
        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "auth": {
                    "type": "jwt",
                    "apiKey": "ibm-api-key",
                    "tokenEndpoint": "https://iam.cloud.ibm.com/identity/token",
                    "grantType": "urn:ibm:params:oauth:grant-type:apikey",
                    "responseType": "cloud_iam",
                },
            }
        )

        assert isinstance(config.auth, JwtTokenProvider)
        assert config.auth.grant_type == "urn:ibm:params:oauth:grant-type:apikey"
        assert config.auth.response_type == "cloud_iam"
