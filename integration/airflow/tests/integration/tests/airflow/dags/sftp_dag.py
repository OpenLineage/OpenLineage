# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="sftp_dag", start_date=days_ago(7), schedule_interval="@once") as dag:
    SFTPOperator(
        task_id="sftp_task",
        ssh_conn_id="sftp_conn",
        local_filepath="/opt/airflow/airflow.cfg",
        remote_filepath="/tmp/airflow.cfg",
    )
