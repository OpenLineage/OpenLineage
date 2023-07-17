# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import re
import uuid
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from openlineage.client.client import OpenLineageClient
from openlineage.client.run import (
    SCHEMA_URL,
    Dataset,
    DatasetEvent,
    Job,
    JobEvent,
    Run,
    RunEvent,
    RunState,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_client_fails_with_wrong_event_type() -> None:
    client = OpenLineageClient(url="http://example.com", session=MagicMock())

    with pytest.raises(
        ValueError,
        match="`emit` only accepts RunEvent, DatasetEvent, JobEvent class",
    ):
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


def test_client_sends_proper_json_with_minimal_run_event() -> None:
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
        f'"69f4acab-b87d-4fc0-b27b-8ea950370ff3"}}, "schemaURL": "{SCHEMA_URL}"}}',
        timeout=5.0,
        verify=True,
    )


def test_client_sends_proper_json_with_minimal_dataset_event() -> None:
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    client.emit(
        DatasetEvent(
            eventTime="2021-11-03T10:53:52.427343",
            producer="producer",
            schemaURL="datasetSchemaUrl",
            dataset=Dataset(namespace="my-namespace", name="my-ds"),
        ),
    )

    session.post.assert_called_with(
        "http://example.com/api/v1/lineage",
        '{"dataset": {"facets": {}, "name": "my-ds", '
        '"namespace": "my-namespace"}, "eventTime": '
        '"2021-11-03T10:53:52.427343", "producer": "producer", '
        '"schemaURL": "datasetSchemaUrl"}',
        timeout=5.0,
        verify=True,
    )


def test_client_sends_proper_json_with_minimal_job_event() -> None:
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    client.emit(
        JobEvent(
            eventTime="2021-11-03T10:53:52.427343",
            schemaURL="jobSchemaURL",
            job=Job("openlineage", "job"),
            producer="producer",
        ),
    )

    session.post.assert_called_with(
        "http://example.com/api/v1/lineage",
        '{"eventTime": "2021-11-03T10:53:52.427343", '
        '"inputs": [], "job": {"facets": {}, "name": "job", "namespace": '
        '"openlineage"}, "outputs": [], "producer": "producer", '
        '"schemaURL": "jobSchemaURL"}',
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
            "schemaURL",
        ),
    )
    client.transport.emit.assert_called_once()


@pytest.mark.parametrize(
    ("name", "config_path", "should_emit"),
    [
        ("job", "exact_filter.yml", False),
        ("wrong", "exact_filter.yml", False),
        ("job1", "exact_filter.yml", True),
        ("1wrong", "exact_filter.yml", True),
        ("asdf", "exact_filter.yml", True),
        ("", "exact_filter.yml", True),
        ("whatever", "regex_filter.yml", False),
        ("something_whatever_asdf", "regex_filter.yml", False),
        ("$$$.whatever", "regex_filter.yml", False),
        ("asdf", "regex_filter.yml", True),
        ("", "regex_filter.yml", True),
    ],
)
def test_client_filters_exact_job_name_events(
    name: str,
    config_path: str,
    root: Path,
    *,
    should_emit: bool,
) -> None:
    with patch.dict(
        os.environ,
        {"OPENLINEAGE_CONFIG": str(root / "config" / config_path)},
    ):
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
            schemaURL="",
        )

        client.emit(event)
        assert transport.emit.called == should_emit


def test_setting_ol_client_log_level() -> None:
    default_log_level = logging.WARNING
    # without environment variable
    OpenLineageClient()
    parent_logger = logging.getLogger("openlineage.client")
    logger = logging.getLogger("openlineage.client.client")
    assert parent_logger.getEffectiveLevel() == default_log_level
    assert logger.getEffectiveLevel() == default_log_level
    with patch.dict(os.environ, {"OPENLINEAGE_CLIENT_LOGGING": "CRITICAL"}):
        assert parent_logger.getEffectiveLevel() == default_log_level
        assert logger.getEffectiveLevel() == default_log_level
        OpenLineageClient()
        assert parent_logger.getEffectiveLevel() == logging.CRITICAL
        assert logger.getEffectiveLevel() == logging.CRITICAL
