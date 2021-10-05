import logging
import attr
from urllib.parse import urljoin, urlparse
from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde

from requests import Session
from requests.adapters import HTTPAdapter


log = logging.getLogger(__name__)


@attr.s
class OpenLineageClientOptions:
    timeout: float = attr.ib(default=5.0)
    verify: bool = attr.ib(default=True)
    api_key: str = attr.ib(default=None)
    adapter: HTTPAdapter = attr.ib(default=None)


class BaseTransport:
    def send(self, event: RunEvent):
        raise NotImplementedError()


class HttpTransport(BaseTransport):
    def __init__(
        self,
        url: str,
        options: OpenLineageClientOptions = OpenLineageClientOptions(),
        session: Session = None
    ):
        parsed = urlparse(url)
        if not (parsed.scheme and parsed.netloc):
            raise ValueError(f"Need valid url for OpenLineageClient, passed {url}")
        self.url = url
        self.options = options
        self.session = session if session else Session()
        self.session.headers['Content-Type'] = 'application/json'
        if self.options.api_key:
            self._add_auth(options.api_key)
        if self.options.adapter:
            self.session.mount(self.url, options.adapter)

    def send(self, event: RunEvent):
        data = Serde.to_json(event)
        log.debug(f"Sending openlineage event {event}")
        resp = self.session.post(
            urljoin(self.url, 'api/v1/lineage'),
            data,
            timeout=self.options.timeout,
            verify=self.options.verify
        )
        resp.raise_for_status()
        return resp

    def _add_auth(self, api_key: str):
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}"
        })
