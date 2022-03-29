# SPDX-License-Identifier: Apache-2.0.
import datetime
import uuid
from unittest.mock import patch

from requests import Session

from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client.serde import Serde
from openlineage.client.transport.http import HttpConfig, HttpTransport


def test_http_loads_full_config():
    config = HttpConfig.from_dict({
        "type": "http",
        "url": "http://backend:5000/api/v1/lineage",
        "verify": False,
        "auth": {
            "type": "api_key",
            "api_key": "1500100900"
        },
    })

    assert config.url == "http://backend:5000/api/v1/lineage"
    assert config.verify is False
    assert config.auth.api_key == "1500100900"
    assert isinstance(config.session, Session)
    assert config.adapter is None


def test_http_loads_minimal_config():
    config = HttpConfig.from_dict({
        "type": "http",
        "url": "http://backend:5000/api/v1/lineage"
    })
    assert config.url == "http://backend:5000/api/v1/lineage"
    assert config.verify is True
    assert not hasattr(config.auth, 'api_key')
    assert isinstance(config.session, Session)
    assert config.adapter is None


@patch("requests.Session")
def test_client_with_http_transport_emits(session):
    config = HttpConfig.from_dict({
        "type": "http",
        "url": "http://backend:5000",
        "session": session
    })
    transport = HttpTransport(config)

    client = OpenLineageClient(transport=transport)
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(uuid.uuid4())),
        job=Job(namespace="http", name="test"),
        producer="prod",
    )

    client.emit(event)
    transport.session.post.assert_called_once_with(
        "http://backend:5000/api/v1/lineage",
        Serde.to_json(event),
        timeout=5.0,
        verify=True
    )
