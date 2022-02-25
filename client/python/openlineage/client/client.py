# SPDX-License-Identifier: Apache-2.0.

import os
import logging
from urllib.parse import urljoin

import attr
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.url import parse_url

from openlineage.client import constants
from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde


@attr.s
class OpenLineageClientOptions:
    timeout: float = attr.ib(default=5.0)
    verify: bool = attr.ib(default=True)
    api_key: str = attr.ib(default=None)
    adapter: HTTPAdapter = attr.ib(default=None)


log = logging.getLogger(__name__)


class OpenLineageClient:
    def __init__(
            self,
            url: str,
            options: OpenLineageClientOptions = OpenLineageClientOptions(),
            session: Session = None
    ):
        url = url.strip()
        try:
            parsed = parse_url(url)
            if not (parsed.scheme and parsed.netloc):
                raise ValueError(f"Need valid url for OpenLineageClient, passed {url}")
        except Exception as e:
            raise ValueError(f"Need valid url for OpenLineageClient, passed {url}. Exception: {e}")
        self.url = url
        self.options = options
        self.session = session if session else Session()
        self.session.headers['Content-Type'] = 'application/json'

        if self.options.api_key:
            self._add_auth(options.api_key)
        if self.options.adapter:
            self.session.mount(self.url, options.adapter)

    def emit(self, event: RunEvent):
        data = Serde.to_json(event)
        if log.isEnabledFor(logging.DEBUG):
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

    @classmethod
    def from_environment(cls):
        server_url = os.getenv("OPENLINEAGE_URL", constants.DEFAULT_OPENLINEAGE_URL)
        if server_url:
            log.info(f"Constructing openlineage client to send events to {server_url}")
        return OpenLineageClient(
            url=server_url,
            options=OpenLineageClientOptions(
                timeout=constants.DEFAULT_TIMEOUT_MS / 1000,
                api_key=os.getenv("OPENLINEAGE_API_KEY", None)
            )
        )
