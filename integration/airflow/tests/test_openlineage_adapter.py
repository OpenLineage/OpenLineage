import datetime
import os
import uuid
from unittest import mock

from requests import RequestException
from openlineage.client.client import OpenLineageClient, HttpTransport
from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import TaskMetadata


def test_create_client_from_marquez_url():
    os.environ['MARQUEZ_URL'] = "http://marquez:5000"
    os.environ['MARQUEZ_API_KEY'] = "test-api-key"

    client = OpenLineageAdapter().get_or_create_openlineage_client()

    assert isinstance(client.transport, HttpTransport)
    assert client.transport.url == "http://marquez:5000"
    assert client.transport.options.api_key == "test-api-key"
    del os.environ['MARQUEZ_URL']
    del os.environ['MARQUEZ_API_KEY']


def test_create_client_from_ol_env():
    os.environ['OPENLINEAGE_URL'] = "http://ol-api:5000"
    os.environ['OPENLINEAGE_API_KEY'] = "api-key"

    client = OpenLineageAdapter().get_or_create_openlineage_client()

    assert isinstance(client.transport, HttpTransport)
    assert client.transport.url == "http://ol-api:5000"
    assert client.transport.options.api_key == "api-key"
    del os.environ['OPENLINEAGE_URL']
    del os.environ['OPENLINEAGE_API_KEY']


@mock.patch.object(OpenLineageClient, "emit")
@mock.patch('openlineage.airflow.adapter.log')
def test_send_events_handle_failure(mock_log, mock_emit):
    mock_emit.side_effect = RequestException
    adapter = OpenLineageAdapter()
    adapter.start_task(
        str(uuid.uuid4()),
        "job_name",
        "",
        datetime.datetime.now().isoformat(),
        str(uuid.uuid4()),
        None,
        datetime.datetime.now().isoformat(),
        datetime.datetime.now().isoformat(),
        TaskMetadata("name"),
        {}
    )
    mock_log.exception.assert_called_once()
