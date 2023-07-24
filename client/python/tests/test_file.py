# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime
import json
import tempfile
import time
import uuid
from os import listdir
from os.path import isfile, join
from typing import Any, Dict, List

from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.transport.file import FileConfig, FileTransport


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
            run=Run(runId=str(uuid.uuid4())),
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

    log_files = [
        join(log_dir.name, f) for f in listdir(log_dir.name) if isfile(join(log_dir.name, f))
    ]
    log_files.sort()

    for i, log_file in enumerate(log_files):
        with open(log_file) as log_file_handle:
            log_line = json.loads(log_file_handle.read())
            test_event = test_event_set[i]

            assert_test_events(log_line, test_event)
