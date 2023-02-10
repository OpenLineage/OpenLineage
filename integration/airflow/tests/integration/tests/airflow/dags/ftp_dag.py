# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from airflow import DAG
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="ftp_dag", start_date=days_ago(7), schedule_interval="@once") as dag:
    FTPFileTransmitOperator(
        task_id="ftp_task",
        ftp_conn_id="ftp_conn",
        local_filepath="/opt/airflow/airflow.cfg",
        remote_filepath="/airflow.cfg",
    )
