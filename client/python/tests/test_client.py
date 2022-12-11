# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock

import pytest

from openlineage.client.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job


def test_client_fails_with_wrong_event_type():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    with pytest.raises(ValueError):
        client.emit("event")


def test_client_fails_to_create_with_wrong_url():
    session = MagicMock()
    with pytest.raises(ValueError):
        OpenLineageClient(url="notanurl", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="http://", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="example.com", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="http:example.com", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="http:/example.com", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="196.168.0.1", session=session)


def test_client_passes_to_create_with_valid_url():
    session = MagicMock()
    assert OpenLineageClient(url="http://196.168.0.1", session=session).transport.url == \
           "http://196.168.0.1"
    assert OpenLineageClient(url="http://196.168.0.1", session=session).transport.url == \
           "http://196.168.0.1"
    assert OpenLineageClient(url="http://example.com  ", session=session).transport.url == \
           "http://example.com"
    assert OpenLineageClient(url=" http://example.com", session=session).transport.url == \
           "http://example.com"
    assert OpenLineageClient(url="  http://marquez:5000  ", session=session).transport.url == \
           "http://marquez:5000"
    assert OpenLineageClient(url="  https://marquez  ", session=session).transport.url == \
           "https://marquez"


def test_client_sends_proper_json_with_minimal_event():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "producer"
        )
    )

    session.post.assert_called_with(
        "http://example.com/api/v1/lineage",
        '{"eventTime": "2021-11-03T10:53:52.427343", "eventType": "START", "inputs": [], "job": '
        '{"facets": {}, "name": "job", "namespace": "openlineage"}, "outputs": [], '
        '"producer": "producer", "run": {"facets": {}, "runId": '
        '"69f4acab-b87d-4fc0-b27b-8ea950370ff3"}}',
        timeout=5.0,
        verify=True
    )


def test_client_uses_passed_transport():
    transport = MagicMock()
    client = OpenLineageClient(transport=transport)
    assert client.transport == transport

    client.emit(event=RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "producer"
        )
    )
    client.transport.emit.assert_called_once()
