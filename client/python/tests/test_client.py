# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import re
import uuid
from unittest.mock import MagicMock, patch

import pytest

from openlineage.client.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState


def test_client_fails_with_wrong_event_type() -> None:
    client = OpenLineageClient(url="http://example.com", session=MagicMock())

    with pytest.raises(ValueError, match="`emit` only accepts RunEvent class"):
        client.emit("event")


@pytest.mark.parametrize(
    "url",
    [
        "notanurl",
        "http://",
        "example.com",
        "http:example.com",
        "http:/example.com",
        "196.168.0.1",
    ],
)
def test_client_fails_to_create_with_wrong_url(url: str) -> None:
    with pytest.raises(ValueError, match=re.escape(url)):
        OpenLineageClient(url=url, session=MagicMock())


@pytest.mark.parametrize(
    ("url", "res"),
    [
        ("http://196.168.0.1", "http://196.168.0.1"),
        ("http://196.168.0.1 ", "http://196.168.0.1"),
        ("http://example.com  ", "http://example.com"),
        (" http://example.com", "http://example.com"),
        ("  http://marquez:5000  ", "http://marquez:5000"),
        ("  https://marquez  ", "https://marquez"),
    ],
)
def test_client_passes_to_create_with_valid_url(url: str, res: str) -> None:
    assert OpenLineageClient(url=url, session=MagicMock()).transport.url == res


def test_client_sends_proper_json_with_minimal_event() -> None:
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "producer",
        ),
    )

    session.post.assert_called_with(
        "http://example.com/api/v1/lineage",
        '{"eventTime": "2021-11-03T10:53:52.427343", "eventType": "START", "inputs": [], "job": '
        '{"facets": {}, "name": "job", "namespace": "openlineage"}, "outputs": [], '
        '"producer": "producer", "run": {"facets": {}, "runId": '
        '"69f4acab-b87d-4fc0-b27b-8ea950370ff3"}}',
        timeout=5.0,
        verify=True,
    )


def test_client_uses_passed_transport() -> None:
    transport = MagicMock()
    client = OpenLineageClient(transport=transport)
    assert client.transport == transport

    client.emit(
        event=RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "producer",
        ),
    )
    client.transport.emit.assert_called_once()


@pytest.mark.parametrize(
    ("name", "config_path", "should_emit"),
    [
        ("job", "tests/config/exact_filter.yml", False),
        ("wrong", "tests/config/exact_filter.yml", False),
        ("job1", "tests/config/exact_filter.yml", True),
        ("1wrong", "tests/config/exact_filter.yml", True),
        ("asdf", "tests/config/exact_filter.yml", True),
        ("", "tests/config/exact_filter.yml", True),
        ("whatever", "tests/config/regex_filter.yml", False),
        ("something_whatever_asdf", "tests/config/regex_filter.yml", False),
        ("$$$.whatever", "tests/config/regex_filter.yml", False),
        ("asdf", "tests/config/regex_filter.yml", True),
        ("", "tests/config/regex_filter.yml", True),
    ],
)
def test_client_filters_exact_job_name_events(
    name: str,
    config_path: str,
    *,
    should_emit: bool,
) -> None:
    with patch.dict(os.environ, {"OPENLINEAGE_CONFIG": config_path}):
        factory = MagicMock()
        transport = MagicMock()
        factory.create.return_value = transport
        client = OpenLineageClient(factory=factory)

        run = Run(runId=str(uuid.uuid4()))
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2021-11-03T10:53:52.427343",
            run=run,
            job=Job(name=name, namespace=""),
            producer="",
        )

        client.emit(event)
        assert transport.emit.called == should_emit
