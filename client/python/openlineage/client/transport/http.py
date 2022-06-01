# SPDX-License-Identifier: Apache-2.0.
import logging
from urllib.parse import urljoin

import attr

from typing import Optional, Dict

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import parse_url

from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields, try_import_subclass_from_string

log = logging.getLogger(__name__)


class TokenProvider:
    def __init__(self, config: Dict):
        pass

    def get_bearer(self) -> Optional[str]:
        return None


class ApiKeyTokenProvider(TokenProvider):
    def __init__(self, config: Dict):
        self.api_key = config['api_key']

    def get_bearer(self) -> Optional[str]:
        return f"Bearer {self.api_key}"


def create_token_provider(auth: Dict) -> TokenProvider:
    if 'type' in auth:
        if auth['type'] == 'api_key':
            return ApiKeyTokenProvider(auth)
        try:
            clazz = try_import_subclass_from_string(auth['type'], TokenProvider)
            return clazz(auth)
        except TypeError:
            pass  # already logged
    return TokenProvider({})


@attr.s
class HttpConfig(Config):
    url: str = attr.ib()
    timeout: float = attr.ib(default=5.0)
    # check TLS certificates
    verify: bool = attr.ib(default=True)
    auth: TokenProvider = attr.ib(factory=lambda: TokenProvider({}))
    # not set by TransportFactory
    session: Optional[Session] = attr.ib(factory=Session)
    # not set by TransportFactory
    adapter: Optional[HTTPAdapter] = attr.ib(default=None)

    @classmethod
    def from_dict(cls, params: dict) -> 'HttpConfig':
        if 'url' not in params:
            raise RuntimeError("`url` key not passed to HttpConfig")
        specified_dict = get_only_specified_fields(cls, params)
        specified_dict['auth'] = create_token_provider(specified_dict.get('auth', {}))
        return cls(**specified_dict)

    @classmethod
    def from_options(cls, url: str, options, session: Optional[Session]) -> 'HttpConfig':
        return cls(
            url=url,
            timeout=options.timeout,
            verify=options.verify,
            auth=ApiKeyTokenProvider({"api_key": options.api_key})
            if options.api_key else TokenProvider({}),
            session=session if session else Session(),
            adapter=options.adapter
        )


class HttpTransport(Transport):
    kind = "http"
    config = HttpConfig

    def __init__(self, config: HttpConfig):
        url = config.url.strip()
        try:
            parsed = parse_url(url)
            if not (parsed.scheme and parsed.netloc):
                raise ValueError(f"Need valid url for OpenLineageClient, passed {url}")
        except Exception as e:
            raise ValueError(f"Need valid url for OpenLineageClient, passed {url}. Exception: {e}")
        self.url = url
        self.session = config.session
        if self.session:
            self.session.headers['Content-Type'] = 'application/json'
        self.timeout = config.timeout
        self.verify = config.verify

        self._add_auth(config.auth)
        if config.adapter:
            self.set_adapter(config.adapter)

    def set_adapter(self, adapter: HTTPAdapter):
        if self.session:
            self.session.mount(self.url, adapter)

    def emit(self, event: RunEvent):
        event = Serde.to_json(event)
        if log.isEnabledFor(logging.DEBUG):
            log.debug(f"Sending openlineage event {event}")
        if self.session:
            resp = self.session.post(
                urljoin(self.url, 'api/v1/lineage'),
                event,
                timeout=self.timeout,
                verify=self.verify
            )
        resp.raise_for_status()
        return resp

    def _add_auth(self, token_provider: TokenProvider):
        if self.session:
            self.session.headers.update({
                "Authorization": token_provider.get_bearer()    # type: ignore
            })
