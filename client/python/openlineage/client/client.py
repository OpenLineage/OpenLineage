# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging

from requests import Session
from openlineage.client import constants
from openlineage.client.run import RunEvent
from openlineage.client.transport import BaseTransport, HttpTransport,  OpenLineageClientOptions

log = logging.getLogger(__name__)


class OpenLineageClient:
    def __init__(
        self,
        url: str = None,
        options: OpenLineageClientOptions = OpenLineageClientOptions(),
        session: Session = None,
        transport: BaseTransport = None
    ):
        """
        Create OpenLineageClient from provided transport. If url is passed, for back compatibility
        construct HttpTransport.
        """
        if url:
            if transport:
                raise RuntimeError(
                    "can't pass url and transport to OpenLineageClient at the same time"
                )
            self.transport = HttpTransport(
                url=url,
                options=options,
                session=session
            )
        elif transport:
            self.transport = transport
        else:
            raise ValueError("Transport not passed to OpenLineageClient")

    def emit(self, event: RunEvent):
        return self.transport.send(event)

    @classmethod
    def from_environment(cls):
        transport = os.getenv(
            "OPENLINEAGE_TRANSPORT",
            constants.DEFAULT_OPENLINEAGE_TRANSPORT
        ).lower()

        if transport == "http":
            server_url = os.getenv("OPENLINEAGE_URL", constants.DEFAULT_OPENLINEAGE_URL)
            log.info(f"Constructing openlineage client to send events to {server_url}")
            return OpenLineageClient(
                transport=HttpTransport(
                    url=server_url,
                    options=OpenLineageClientOptions(
                        timeout=constants.DEFAULT_TIMEOUT_MS / 1000,
                        api_key=os.getenv("OPENLINEAGE_API_KEY", None)
                    )
                )
            )
        raise ValueError(f"transport {transport} not implemented")
