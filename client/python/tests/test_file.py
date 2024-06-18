# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime
import json
import os
import tempfile
import time
from os import listdir
from os.path import exists, isfile, join
from typing import Any, Dict, List
from unittest import mock

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.uuid import generate_new_uuid


def emit_test_events(client: OpenLineageClient, debuff_time: int = 0) -> List[Dict[str, Any]]:
    test_event_set = [
        {
            "eventType": RunState.START,
            "name": "test",
            "namespace": "file",
            "producer": "prod",
        },
        {
            "eventType": RunState.COMPLETE,
            "name": "test",
            "namespace": "file",
            "producer": "prod",
        },
    ]

    for test_event in test_event_set:
        event = RunEvent(
            eventType=test_event["eventType"],
            eventTime=datetime.datetime.now().isoformat(),
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace=test_event["namespace"], name=test_event["name"]),
            producer=test_event["producer"],
        )

        client.emit(event)
        time.sleep(debuff_time)  # avoid writing on the same timestamp

    return test_event_set


def assert_test_events(log_line: List[Dict[str, Any]], test_event: List[Dict[str, Any]]) -> None:
    assert log_line["eventType"] == test_event["eventType"].name
    assert log_line["job"]["name"] == test_event["name"]
    assert log_line["job"]["namespace"] == test_event["namespace"]
    assert log_line["producer"] == test_event["producer"]


def test_client_with_file_transport_append_emits() -> None:
    log_file = tempfile.NamedTemporaryFile()
    config = FileConfig(append=True, log_file_path=log_file.name)
    transport = FileTransport(config)

    client = OpenLineageClient(transport=transport)

    test_event_set = emit_test_events(client)

    with open(log_file.name) as log_file_handle:
        log_file_handle.seek(0)
        log_lines = log_file_handle.read().splitlines()

        for i, raw_log_line in enumerate(log_lines):
            log_line = json.loads(raw_log_line)
            test_event = test_event_set[i]
            assert_test_events(log_line, test_event)


def test_client_with_file_transport_write_emits() -> None:
    log_dir = tempfile.TemporaryDirectory()

    config = FileConfig(log_file_path=join(log_dir.name, "logtest"))
    transport = FileTransport(config)

    client = OpenLineageClient(transport=transport)

    test_event_set = emit_test_events(client, 0.01)

    log_files = [join(log_dir.name, f) for f in listdir(log_dir.name) if isfile(join(log_dir.name, f))]
    log_files.sort()

    for i, log_file in enumerate(log_files):
        with open(log_file) as log_file_handle:
            log_line = json.loads(log_file_handle.read())
            test_event = test_event_set[i]

            assert_test_events(log_line, test_event)


@pytest.mark.parametrize(
    "append",
    [
        True,
        False,
    ],
)
@mock.patch("openlineage.client.transport.file.datetime")
def test_file_transport_raises_error_with_non_writeable_file(mock_dt, append) -> None:
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="file", name="test"),
        producer="prod",
    )
    mock_dt.now().strftime.return_value = "timestr"

    with tempfile.TemporaryDirectory() as log_dir:
        log_base_file_path = join(log_dir, "logtest")

        config = FileConfig(log_file_path=log_base_file_path, append=append)
        transport = FileTransport(config)

        log_file_path = log_base_file_path if append else f"{log_base_file_path}-timestr"

        # Create the file and make it read-only
        with open(log_file_path, "w") as f:
            f.write("This is a test file.")
        os.chmod(log_file_path, 0o444)

        with pytest.raises(RuntimeError, match=f"Log file `{log_file_path}` is not writeable"):
            transport.emit(event)

        os.remove(log_file_path)


@pytest.mark.parametrize(
    "append",
    [
        True,
        False,
    ],
)
def test_file_config_from_dict(append) -> None:
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    config = FileConfig.from_dict(params={"log_file_path": file_path, "append": append})
    assert config.append is append
    assert config.log_file_path == file_path


def test_file_config_from_dict_no_file() -> None:
    with pytest.raises(RuntimeError):
        FileConfig.from_dict({})


@pytest.mark.parametrize(
    "append",
    [
        True,
        False,
    ],
)
def test_file_config_does_not_create_file_if_missing(append) -> None:
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "test")

    assert not exists(file_path)
    FileConfig.from_dict(params={"log_file_path": file_path, "append": append})
    assert not exists(file_path)


@pytest.mark.parametrize(
    "append",
    [
        True,
        False,
    ],
)
def test_file_config_append_mode_does_not_modify_file_if_exists(append) -> None:
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "test-file")

    with open(file_path, "w") as file:
        file.write("test content")

    assert exists(file_path)
    FileConfig.from_dict(params={"log_file_path": file_path, "append": append})
    assert exists(file_path)

    with open(file_path) as file:
        file_content = file.read()
    assert file_content == "test content"

    os.remove(file_path)
