# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import inspect
import logging
import warnings
from typing import TYPE_CHECKING, Any, Union
from urllib.parse import urljoin

import attr

if TYPE_CHECKING:
    from requests.adapters import HTTPAdapter, Response

    from openlineage.client.client import OpenLineageClientOptions
    from openlineage.client.run import DatasetEvent, JobEvent, RunEvent

from requests import Session

from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields, try_import_from_string

log = logging.getLogger(__name__)


class TokenProvider:
    def __init__(self, config: dict[str, str]) -> None:
        ...

    def get_bearer(self) -> str | None:
        return None


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


@attr.s
class HttpConfig(Config):
    url: str = attr.ib()
    endpoint: str = attr.ib(default="api/v1/lineage")
    timeout: float = attr.ib(default=5.0)
    # check TLS certificates
    verify: bool = attr.ib(default=True)
    auth: TokenProvider = attr.ib(factory=lambda: TokenProvider({}))
    # not set by TransportFactory
    session: Session | None = attr.ib(default=None)
    # not set by TransportFactory
    adapter: HTTPAdapter | None = attr.ib(default=None)

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> HttpConfig:
        if "url" not in params:
            msg = "`url` key not passed to HttpConfig"
            raise RuntimeError(msg)
        specified_dict = get_only_specified_fields(cls, params)
        specified_dict["auth"] = create_token_provider(specified_dict.get("auth", {}))
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
            auth=ApiKeyTokenProvider({"api_key": options.api_key})
            if options.api_key
            else TokenProvider({}),
            session=session,
            adapter=options.adapter,
        )


class HttpTransport(Transport):
    kind = "http"
    config_class = HttpConfig

    def __init__(self, config: HttpConfig) -> None:
        url = config.url.strip()
        self.config = config

        log.debug("Constructing openlineage client to send events to %s - config %s", url, config)
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
        self.session = None
        if config.session:
            self.session = config.session
            self.session.headers["Content-Type"] = "application/json"
            auth_headers = self._auth_headers(config.auth)
            self.session.headers.update(auth_headers)
        self.timeout = config.timeout
        self.verify = config.verify

        if config.adapter:
            self.set_adapter(config.adapter)

    def set_adapter(self, adapter: HTTPAdapter) -> None:
        if self.session:
            self.session.mount(self.url, adapter)

    def emit(self, event: Union[RunEvent, DatasetEvent, JobEvent]) -> Response:  # noqa: UP007
        event_str = Serde.to_json(event)
        if self.session:
            resp = self.session.post(
                urljoin(self.url, self.endpoint),
                event_str,
                timeout=self.timeout,
                verify=self.verify,
            )
        else:
            headers = {
                "Content-Type": "application/json",
            }
            headers.update(self._auth_headers(self.config.auth))
            with Session() as session:
                resp = session.post(
                    urljoin(self.url, self.endpoint),
                    event_str,
                    headers=headers,
                    timeout=self.timeout,
                    verify=self.verify,
                )
            resp.close()
        resp.raise_for_status()
        return resp

    def _auth_headers(self, token_provider: TokenProvider) -> dict:  # type: ignore[type-arg]
        bearer = token_provider.get_bearer()
        if bearer:
            return {"Authorization": bearer}
        return {}
