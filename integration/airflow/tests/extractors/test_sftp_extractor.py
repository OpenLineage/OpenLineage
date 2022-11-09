# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import paramiko
import pytest
import socket

from unittest import mock

from airflow.models import Connection, DAG
from airflow.providers.sftp.operators.sftp import SFTPOperation
from airflow.utils import timezone

from openlineage.airflow.extractors.sftp_extractor import SFTPExtractor
from openlineage.airflow.utils import try_import_from_string
from openlineage.common.dataset import Source, Dataset

SFTPOperator = try_import_from_string("airflow.providers.sftp.operators.sftp.SFTPOperator")

SCHEME = "file"

LOCAL_FILEPATH = "/path/to/local"
LOCAL_HOST = socket.gethostname()
LOCAL_SOURCE = Source(
    scheme=SCHEME, authority=LOCAL_HOST,
    connection_url=f"{SCHEME}://{LOCAL_HOST}{LOCAL_FILEPATH}"
)
LOCAL_DATASET = [Dataset(source=LOCAL_SOURCE, name=LOCAL_FILEPATH).to_openlineage_dataset()]

REMOTE_FILEPATH = "/path/to/remote"
REMOTE_HOST = "remotehost"
REMOTE_SOURCE = Source(
    scheme=SCHEME, authority=REMOTE_HOST,
    connection_url=f"{SCHEME}://{REMOTE_HOST}{REMOTE_FILEPATH}"
)
REMOTE_DATASET = [Dataset(source=REMOTE_SOURCE, name=REMOTE_FILEPATH).to_openlineage_dataset()]

CONN_ID = "sftp_default"
CONN = Connection(
    conn_id=CONN_ID,
    conn_type='sftp',
    host=REMOTE_HOST,
    port='22',
)


@pytest.mark.parametrize("operation, expected", [
    (SFTPOperation.GET, (REMOTE_DATASET, LOCAL_DATASET)),
    (SFTPOperation.PUT, (LOCAL_DATASET, REMOTE_DATASET)),
])
@mock.patch('airflow.providers.ssh.hooks.ssh.SSHHook.get_conn', spec=paramiko.SSHClient)
@mock.patch('airflow.providers.ssh.hooks.ssh.SSHHook.get_connection', spec=Connection)
def test_extract_get(get_connection, get_conn, operation, expected):
    get_connection.return_value = CONN

    dag_id = "sftp_dag"
    task_id = "sftp_task"

    task = SFTPOperator(
        task_id=task_id,
        ssh_conn_id=CONN_ID,
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=operation,
    )
    task_metadata = SFTPExtractor(task).extract()

    assert task_metadata.name == f"{dag_id}.{task_id}"
    assert task_metadata.inputs == expected[0]
    assert task_metadata.outputs == expected[1]
