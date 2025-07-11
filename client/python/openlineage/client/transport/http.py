# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import gzip
import http.client as http_client
import inspect
import logging
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import attr
import urllib3.util
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields, try_import_from_string
from requests import Session
from requests.adapters import HTTPAdapter

if TYPE_CHECKING:
    from openlineage.client.client import Event, OpenLineageClientOptions
    from requests import Response


log = logging.getLogger(__name__)


class TokenProvider:
    def __init__(self, config: dict[str, str]) -> None:
        ...

    def get_bearer(self) -> str | None:
        return None


class HttpCompression(Enum):
    GZIP = "gzip"

    def __str__(self) -> str:
        return self.value


class ApiKeyTokenProvider(TokenProvider):
    def __init__(self, config: dict[str, str]) -> None:
        super().__init__(config)
        try:
            self.api_key = config["api_key"]
            msg = "'api_key' option is deprecated, please use 'apiKey'"
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        except KeyError:
            self.api_key = config["apiKey"]

    def get_bearer(self) -> str | None:
        return f"Bearer {self.api_key}"


def create_token_provider(auth: dict[str, str]) -> TokenProvider:
    if "type" in auth:
        if auth["type"] == "api_key":
            return ApiKeyTokenProvider(auth)

        of_type: str = auth["type"]
        subclass = try_import_from_string(of_type)
        if inspect.isclass(subclass) and issubclass(subclass, TokenProvider):
            return subclass(auth)

    return TokenProvider({})


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
    retry: dict[str, Any] = attr.field(
        default={
            "total": 5,
            "read": 5,
            "connect": 5,
            "backoff_factor": 0.3,
            "status_forcelist": [500, 502, 503, 504],
            "allowed_methods": ["HEAD", "POST"],
        }
    )

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
        self.session = config.session  # type: ignore[assignment]

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
            return gzip.compress(event_str.encode("utf-8")), headers

        return event_str, headers
