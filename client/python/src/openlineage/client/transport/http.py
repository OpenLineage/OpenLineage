# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import gzip
import http.client as http_client
import inspect
import json
import logging
import time
from enum import Enum
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import attr
import requests
import urllib3.util
from openlineage.client.serde import Serde
from openlineage.client.transport.http_common import DEFAULT_RETRY_CONFIG
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields, import_from_string
from requests import Session
from requests.adapters import HTTPAdapter

if TYPE_CHECKING:
    from openlineage.client.client import Event, OpenLineageClientOptions
    from requests import Response


log = logging.getLogger(__name__)


class TokenProvider:
    def __init__(self, config: dict[str, str]) -> None: ...

    def get_bearer(self) -> str | None:
        return None


class HttpCompression(Enum):
    GZIP = "gzip"

    def __str__(self) -> str:
        return self.value


class ApiKeyTokenProvider(TokenProvider):
    def __init__(self, config: dict[str, str]) -> None:
        super().__init__(config)
        self.api_key = config.get("apiKey") or config.get("apikey") or config.get("api_key")
        if not self.api_key:
            msg = "apiKey is required for HTTP Transport when auth type is `api_key`."
            raise KeyError(msg)

    def get_bearer(self) -> str | None:
        return f"Bearer {self.api_key}"


class JwtTokenProvider(TokenProvider):
    """TokenProvider that exchanges an API key for a JWT token via a POST endpoint.

    Sends the API key and OAuth parameters as URL-encoded form data.

    The provider automatically tries multiple common JSON field names for the token:
    the configured token_fields (default ["token", "access_token"]). This ensures
    compatibility with various OAuth providers.

    The provider caches tokens and automatically refreshes them before expiry. By default,
    tokens are refreshed 120 seconds before they expire. This can be configured using the
    tokenRefreshBuffer parameter.

    Configuration example:
        {
            "type": "jwt",
            "apiKey": "your-api-key",
            "tokenEndpoint": "https://auth.example.com/token",
            "tokenFields": ["token", "access_token"],  # optional
            "expiresInField": "expires_in",  # optional
            "grantType": "urn:ietf:params:oauth:grant-type:jwt-bearer",  # optional
            "responseType": "token",  # optional
            "tokenRefreshBuffer": 120  # optional, defaults to 120 seconds
        }

    For IBM Cloud IAM, use these settings:
        {
            "type": "jwt",
            "apiKey": "your-ibm-api-key",
            "tokenEndpoint": "https://iam.cloud.ibm.com/identity/token",
            "grantType": "urn:ibm:params:oauth:grant-type:apikey",
            "responseType": "cloud_iam"
        }
    """

    TOKEN_REFRESH_BUFFER_SECONDS = 120  # Default: Refresh 120s before expiry

    def __init__(self, config: dict[str, str]) -> None:
        super().__init__(config)
        self.api_key = config.get("apiKey") or config.get("apikey") or config.get("api_key")
        if not self.api_key:
            msg = "apiKey is required for JWT token provider."
            raise KeyError(msg)

        # Support multiple naming conventions for backwards compatibility
        # Preferred: token_endpoint (from TOKEN_ENDPOINT env var)
        # Also support: tokenEndpoint, tokenendpoint
        token_endpoint = config.get("token_endpoint") or config.get("tokenEndpoint")
        if not token_endpoint:
            msg = "tokenEndpoint is required for JWT token provider."
            raise KeyError(msg)
        self.token_endpoint: str = str(token_endpoint)

        # Token field names to try (in order)
        token_fields = config.get("tokenFields") or config.get("token_fields")
        if token_fields:
            self.token_fields = token_fields if isinstance(token_fields, list) else [token_fields]
        else:
            self.token_fields = ["token", "access_token"]

        # Expiration field name
        self.expires_in_field = config.get("expiresInField") or config.get("expires_in_field") or "expires_in"

        # OAuth parameters
        self.grant_type = (
            config.get("grantType")
            or config.get("grant_type")
            or "urn:ietf:params:oauth:grant-type:jwt-bearer"
        )
        self.response_type = config.get("responseType") or config.get("response_type") or "token"

        # Token refresh buffer (seconds before expiry to refresh)
        token_refresh_buffer = config.get("tokenRefreshBuffer") or config.get("token_refresh_buffer")
        if token_refresh_buffer:
            try:
                self.token_refresh_buffer = int(token_refresh_buffer)
            except ValueError:
                log.warning(
                    "Invalid tokenRefreshBuffer value %s, using default: %s instead",
                    token_refresh_buffer,
                    self.TOKEN_REFRESH_BUFFER_SECONDS,
                )
                self.token_refresh_buffer = self.TOKEN_REFRESH_BUFFER_SECONDS
        else:
            self.token_refresh_buffer = self.TOKEN_REFRESH_BUFFER_SECONDS

        # Token cache
        self._cached_token: str | None = None
        self._token_expiry: float | None = None

    def get_bearer(self) -> str | None:
        """Get the bearer token, fetching a new one if needed."""
        if self._is_token_valid():
            return f"Bearer {self._cached_token}"

        try:
            self._fetch_token()
            return f"Bearer {self._cached_token}" if self._cached_token else None
        except Exception as e:
            log.error("Failed to fetch JWT token: %s", e)
            raise

    def _is_token_valid(self) -> bool:
        """Check if the cached token is still valid."""
        if not self._cached_token or not self._token_expiry:
            return False
        # Refresh if within buffer time of expiry
        return self._get_current_time() < (self._token_expiry - self.token_refresh_buffer)

    def _get_current_time(self) -> float:
        """Get current time in seconds. Can be overridden for testing."""
        return time.time()

    def _fetch_token(self) -> None:
        """Fetch a new JWT token from the token endpoint."""

        # Prepare form data
        data = {
            "grant_type": self.grant_type,
            "response_type": self.response_type,
            "apikey": self.api_key,
        }

        try:
            response = requests.post(
                self.token_endpoint,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            response.raise_for_status()

            response_json = response.json()

            # Try to find token in response using configured field names
            token = self._extract_token_from_response(response_json)
            if not token:
                msg = f"Token not found in response. Tried fields: {self.token_fields}"
                raise ValueError(msg)

            self._cached_token = token

            # Try to get expiration from response
            expires_in = self._extract_expires_in(response_json)

            if expires_in:
                self._token_expiry = self._get_current_time() + expires_in
            else:
                # Try to extract expiration from JWT payload
                self._token_expiry = self._extract_expiry_from_jwt(token)

            log.debug("Successfully fetched JWT token, expires at: %s", self._token_expiry)

        except Exception as e:
            msg = f"Failed to fetch JWT token from {self.token_endpoint}: {e}"
            raise RuntimeError(msg) from e

    def _extract_token_from_response(self, response_json: dict[str, Any]) -> str | None:
        """Extract token from response JSON using configured field names."""
        for field in self.token_fields:
            # Try exact match first
            if field in response_json:
                value = response_json[field]
                return str(value) if value is not None else None

            # Try case-insensitive match
            for key in response_json:
                if key.lower() == field.lower():
                    value = response_json[key]
                    return str(value) if value is not None else None

        return None

    def _extract_expires_in(self, response_json: dict[str, Any]) -> int | None:
        """Extract expiration time from response JSON."""
        # Try exact match
        if self.expires_in_field in response_json:
            return int(response_json[self.expires_in_field])

        # Try case-insensitive match and handle camelCase/snake_case variations
        for key in response_json:
            if key.lower() == self.expires_in_field.lower():
                return int(response_json[key])
            # Handle camelCase to snake_case (e.g., expiresIn -> expires_in)
            if key.lower().replace("_", "") == self.expires_in_field.lower().replace("_", ""):
                return int(response_json[key])

        return None

    def _extract_expiry_from_jwt(self, token: str) -> float | None:
        """Extract expiration time from JWT payload's 'exp' claim."""
        try:
            # JWT format: header.payload.signature
            parts = token.split(".")
            if len(parts) != 3:
                return None

            # Decode payload (add padding if needed)
            payload = parts[1]
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += "=" * padding

            decoded = base64.urlsafe_b64decode(payload)
            payload_json = json.loads(decoded)

            if "exp" in payload_json:
                return float(payload_json["exp"])

        except Exception as e:
            log.debug("Failed to extract expiry from JWT: %s", e)

        return None


def create_token_provider(auth: dict[str, str]) -> TokenProvider:
    if "type" not in auth:
        log.debug("No auth type specified, fallback to default TokenProvider")
        return TokenProvider({})

    if auth["type"] == "api_key":
        log.debug("Using ApiKeyTokenProvider")
        return ApiKeyTokenProvider(auth)

    if auth["type"] == "jwt":
        log.debug("Using JwtTokenProvider")
        return JwtTokenProvider(auth)

    of_type: str = auth["type"]
    subclass = import_from_string(of_type)

    if not inspect.isclass(subclass):
        raise TypeError(f"Expected token provider {subclass} to be a class")
    if not issubclass(subclass, TokenProvider):
        raise TypeError(f"{subclass} is not a subclass of TokenProvider")

    log.debug("Using %s as token provider", subclass)
    return subclass(auth)


def get_session() -> Session:
    from requests import Session

    return Session()


@attr.define
class HttpConfig(Config):
    url: str
    endpoint: str = "api/v1/lineage"
    timeout: float = 5.0
    # check TLS certificates
    verify: bool = True
    auth: TokenProvider = attr.field(factory=lambda: TokenProvider({}))
    compression: HttpCompression | None = None
    # not set by TransportFactory
    session: Session | None = None
    # not set by TransportFactory
    adapter: HTTPAdapter | None = None
    # custom headers support
    custom_headers: dict[str, str] = attr.field(factory=dict)
    # retry settings
    retry: dict[str, Any] = attr.field(default=DEFAULT_RETRY_CONFIG)

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> HttpConfig:
        if "url" not in params:
            msg = "`url` key not passed to HttpConfig"
            raise RuntimeError(msg)
        specified_dict = get_only_specified_fields(cls, params)
        specified_dict["auth"] = create_token_provider(specified_dict.get("auth", {}))
        compression = specified_dict.get("compression")
        if compression:
            specified_dict["compression"] = HttpCompression(compression)

        # Merge retry config with defaults to preserve unspecified values
        if "retry" in specified_dict:
            specified_dict["retry"] = {**DEFAULT_RETRY_CONFIG, **specified_dict["retry"]}

        return cls(**specified_dict)

    @classmethod
    def from_options(
        cls,
        url: str,
        options: OpenLineageClientOptions,
        session: Session | None,
    ) -> HttpConfig:
        return cls(
            url=url,
            timeout=options.timeout,
            verify=options.verify,
            auth=ApiKeyTokenProvider({"api_key": options.api_key}) if options.api_key else TokenProvider({}),
            session=session,
            adapter=options.adapter,
        )


class HttpTransport(Transport):
    kind = "http"
    config_class = HttpConfig

    def __init__(self, config: HttpConfig) -> None:
        url = config.url.strip()
        self.config = config

        log.debug(
            "Constructing OpenLineage transport that will send events "
            "to HTTP endpoint `%s` using the following config: %s",
            urljoin(url, config.endpoint),
            config,
        )
        try:
            from urllib3.util import parse_url

            parsed = parse_url(url)
        except Exception as e:  # noqa: BLE001
            msg = f"Need valid url for OpenLineageClient, passed {url}. Exception: {e}"
            raise ValueError(msg) from None
        else:
            if not (parsed.scheme and parsed.netloc):
                msg = f"Need valid url for OpenLineageClient, passed {url}"
                raise ValueError(msg)
        self.url = url
        self.endpoint = config.endpoint
        self.timeout = config.timeout
        self.verify = config.verify
        self.compression = config.compression
        self._session: Session | None = None
        self.session = config.session

    def emit(self, event: Event) -> Response:
        # If anyone overrides debuglevel manually, we can potentially leak secrets to logs.
        # Override this setting to make sure it does not happen.
        prev_debuglevel = http_client.HTTPConnection.debuglevel
        http_client.HTTPConnection.debuglevel = 0
        body, headers = self._prepare_request(Serde.to_json(event))

        resp = self.session.post(
            url=urljoin(self.url, self.endpoint),
            data=body,
            headers=headers,
            timeout=self.timeout,
            verify=self.verify,
        )
        resp.close()
        http_client.HTTPConnection.debuglevel = prev_debuglevel
        resp.raise_for_status()
        return resp

    @property
    def session(self) -> Session:
        if not self._session:
            self._session = Session()
            self._prepare_session(self._session)
        return self._session

    @session.setter
    def session(self, value: Session | None) -> None:
        if value:
            self._prepare_session(value)
        self._session = value

    def close(self, timeout: float = -1) -> bool:
        if self._session:
            self._session.close()
            self._session = None
        return True

    def _auth_headers(self, token_provider: TokenProvider) -> dict:  # type: ignore[type-arg]
        bearer = token_provider.get_bearer()
        if bearer:
            return {"Authorization": bearer}
        return {}

    def _prepare_session(self, session: Session) -> None:
        if self.config.adapter:
            session.mount(self.url, self.config.adapter)
        else:
            session.mount(self.url, self._prepare_adapter())

    def _prepare_adapter(self) -> HTTPAdapter:
        retry = urllib3.util.Retry(**self.config.retry)
        return HTTPAdapter(max_retries=retry)

    def _prepare_request(self, event_str: str) -> tuple[bytes | str, dict[str, str]]:
        headers = {
            "Content-Type": "application/json",
            **self._auth_headers(self.config.auth),
            **self.config.custom_headers,
        }
        if self.compression == HttpCompression.GZIP:
            headers["Content-Encoding"] = "gzip"
            # levels higher than 3 are twice as slow:
            # https://github.com/python/cpython/issues/91349#issuecomment-2737161048
            return gzip.compress(event_str.encode("utf-8"), compresslevel=3), headers

        return event_str, headers
