# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import socket
from unittest import mock

import paramiko
import pytest
from airflow.models import DAG, Connection
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperation
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers_manager import ProvidersManager
from airflow.utils import timezone
from openlineage.airflow.extractors.sftp_extractor import SFTPExtractor
from openlineage.airflow.utils import try_import_from_string
from openlineage.common.dataset import Dataset, Source
from packaging.version import Version

SFTP_PROVIDER_VERSION = Version(ProvidersManager().providers["apache-airflow-providers-sftp"].version)

SFTPOperator = try_import_from_string("airflow.providers.sftp.operators.sftp.SFTPOperator")

SCHEME = "file"

LOCAL_FILEPATH = "/path/to/local"
LOCAL_HOST = socket.gethostbyname("localhost")
LOCAL_AUTHORITY = f"{LOCAL_HOST}:{paramiko.config.SSH_PORT}"

REMOTE_FILEPATH = "/path/to/remote"
REMOTE_HOST = "remotehost"
REMOTE_PORT = 22
REMOTE_AUTHORITY = f"{REMOTE_HOST}:{REMOTE_PORT}"

CONN_ID = "sftp_default"
CONN = Connection(
    conn_id=CONN_ID,
    conn_type="sftp",
    host=REMOTE_HOST,
    port=REMOTE_PORT,
)


def get_local_dataset():
    """Create local dataset at runtime to ensure correct producer is set."""
    local_source = Source(
        scheme=SCHEME,
        authority=LOCAL_AUTHORITY,
        connection_url=f"{SCHEME}://{LOCAL_AUTHORITY}{LOCAL_FILEPATH}",
    )
    return [Dataset(source=local_source, name=LOCAL_FILEPATH).to_openlineage_dataset()]


def get_remote_dataset():
    """Create remote dataset at runtime to ensure correct producer is set."""
    remote_source = Source(
        scheme=SCHEME,
        authority=REMOTE_AUTHORITY,
        connection_url=f"{SCHEME}://{REMOTE_AUTHORITY}{REMOTE_FILEPATH}",
    )
    return [Dataset(source=remote_source, name=REMOTE_FILEPATH).to_openlineage_dataset()]


@pytest.mark.parametrize(
    "operation",
    [SFTPOperation.GET, SFTPOperation.PUT],
)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn", spec=paramiko.SSHClient)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection", spec=Connection)
@mock.patch("openlineage.airflow.extractors.sftp_extractor.socket.gethostname", return_value="localhost")
def test_extract_ssh_conn_id(mock_hostname, get_connection, get_conn, operation):
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
    if operation == SFTPOperation.GET:
        assert task_metadata.inputs == get_remote_dataset()
        assert task_metadata.outputs == get_local_dataset()
    else:  # PUT
        assert task_metadata.inputs == get_local_dataset()
        assert task_metadata.outputs == get_remote_dataset()


@pytest.mark.skipif(
    Version("4.0.0") > SFTP_PROVIDER_VERSION,
    reason="SFTPOperator doesn't support sftp_hook as a constructor parameter in this version.",
)
@pytest.mark.parametrize(
    "operation",
    [SFTPOperation.GET, SFTPOperation.PUT],
)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn", spec=paramiko.SSHClient)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection", spec=Connection)
@mock.patch("openlineage.airflow.extractors.sftp_extractor.socket.gethostname", return_value="localhost")
def test_extract_sftp_hook(mock_hostname, get_connection, get_conn, operation):
    get_connection.return_value = CONN

    dag_id = "sftp_dag"
    task_id = "sftp_task"

    task = SFTPOperator(
        task_id=task_id,
        sftp_hook=SFTPHook(ssh_conn_id=CONN_ID),
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=operation,
    )
    task_metadata = SFTPExtractor(task).extract()

    assert task_metadata.name == f"{dag_id}.{task_id}"
    if operation == SFTPOperation.GET:
        assert task_metadata.inputs == get_remote_dataset()
        assert task_metadata.outputs == get_local_dataset()
    else:  # PUT
        assert task_metadata.inputs == get_local_dataset()
        assert task_metadata.outputs == get_remote_dataset()


@pytest.mark.parametrize(
    "operation",
    [SFTPOperation.GET, SFTPOperation.PUT],
)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn", spec=paramiko.SSHClient)
@mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection", spec=Connection)
@mock.patch("openlineage.airflow.extractors.sftp_extractor.socket.gethostname", return_value="localhost")
def test_extract_ssh_hook(mock_hostname, get_connection, get_conn, operation):
    get_connection.return_value = CONN

    dag_id = "sftp_dag"
    task_id = "sftp_task"

    task = SFTPOperator(
        task_id=task_id,
        ssh_hook=SSHHook(ssh_conn_id=CONN_ID),
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=operation,
    )
    task_metadata = SFTPExtractor(task).extract()

    assert task_metadata.name == f"{dag_id}.{task_id}"
    if operation == SFTPOperation.GET:
        assert task_metadata.inputs == get_remote_dataset()
        assert task_metadata.outputs == get_local_dataset()
    else:  # PUT
        assert task_metadata.inputs == get_local_dataset()
        assert task_metadata.outputs == get_remote_dataset()
