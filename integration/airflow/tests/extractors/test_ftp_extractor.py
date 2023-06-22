# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import socket
from unittest import mock

import pytest
from openlineage.airflow.extractors.ftp_extractor import FTPExtractor
from openlineage.airflow.utils import try_import_from_string
from openlineage.common.dataset import Dataset, Source

from airflow.models import DAG, Connection
from airflow.utils import timezone

FTPOperator = try_import_from_string(
    "airflow.providers.ftp.operators.ftp.FTPFileTransmitOperator"
)
FTPOperation = try_import_from_string("airflow.providers.ftp.operators.ftp.FTPOperation")

SCHEME = "file"

LOCAL_FILEPATH = "/path/to/local"
LOCAL_HOST = socket.gethostbyname(socket.gethostname())
LOCAL_PORT = 21
LOCAL_AUTHORITY = f"{LOCAL_HOST}:{LOCAL_PORT}"
LOCAL_SOURCE = Source(
    scheme=SCHEME, authority=LOCAL_AUTHORITY,
    connection_url=f"{SCHEME}://{LOCAL_AUTHORITY}{LOCAL_FILEPATH}"
)
LOCAL_DATASET = [Dataset(source=LOCAL_SOURCE, name=LOCAL_FILEPATH).to_openlineage_dataset()]

REMOTE_FILEPATH = "/path/to/remote"
REMOTE_HOST = "remotehost"
REMOTE_PORT = 21
REMOTE_AUTHORITY = f"{REMOTE_HOST}:{REMOTE_PORT}"
REMOTE_SOURCE = Source(
    scheme=SCHEME, authority=REMOTE_AUTHORITY,
    connection_url=f"{SCHEME}://{REMOTE_AUTHORITY}{REMOTE_FILEPATH}"
)
REMOTE_DATASET = [Dataset(source=REMOTE_SOURCE, name=REMOTE_FILEPATH).to_openlineage_dataset()]

CONN_ID = "ftp_default"
CONN = Connection(
    conn_id=CONN_ID,
    conn_type='ftp',
    host=REMOTE_HOST,
    port=REMOTE_PORT,
)


@pytest.mark.skipif(
    FTPOperator is None,
    reason="FTPFileTransmitOperator is only available since apache-airflow-providers-ftp 3.3.0+."
)
@mock.patch('airflow.providers.ftp.hooks.ftp.FTPHook.get_conn', spec=Connection)
def test_extract_get(get_conn):
    get_conn.return_value = CONN

    dag_id = "ftp_dag"
    task_id = "ftp_task"

    task = FTPOperator(
        task_id=task_id,
        ftp_conn_id=CONN_ID,
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=FTPOperation.GET,
    )
    task_metadata = FTPExtractor(task).extract()

    assert task_metadata.name == f"{dag_id}.{task_id}"
    assert task_metadata.inputs == REMOTE_DATASET
    assert task_metadata.outputs == LOCAL_DATASET


@pytest.mark.skipif(
    FTPOperator is None,
    reason="FTPFileTransmitOperator is only available since apache-airflow-providers-ftp 3.3.0+."
)
@mock.patch('airflow.providers.ftp.hooks.ftp.FTPHook.get_conn', spec=Connection)
def test_extract_put(get_conn):
    get_conn.return_value = CONN

    dag_id = "ftp_dag"
    task_id = "ftp_task"

    task = FTPOperator(
        task_id=task_id,
        ftp_conn_id=CONN_ID,
        dag=DAG(dag_id),
        start_date=timezone.utcnow(),
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=FTPOperation.PUT,
    )
    task_metadata = FTPExtractor(task).extract()

    assert task_metadata.name == f"{dag_id}.{task_id}"
    assert task_metadata.inputs == LOCAL_DATASET
    assert task_metadata.outputs == REMOTE_DATASET
