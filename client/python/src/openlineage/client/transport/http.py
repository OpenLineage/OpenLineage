# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import gzip
import http.client as http_client
import inspect
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import attr
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


def create_token_provider(auth: dict[str, str]) -> TokenProvider:
    if "type" not in auth:
        log.debug("No auth type specified, fallback to default TokenProvider")
        return TokenProvider({})

    if auth["type"] == "api_key":
        log.debug("Using ApiKeyTokenProvider")
        return ApiKeyTokenProvider(auth)

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
