# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.utils.dates import days_ago


with DAG(dag_id="s3copy_dag", start_date=days_ago(7), schedule_interval="@once") as dag:
    S3CopyObjectOperator(
        task_id="s3copy_task",
        aws_conn_id="aws_local_conn",
        source_bucket_name="testbucket",
        dest_bucket_name="testbucket",
        source_bucket_key="testfile",
        dest_bucket_key="testfile2",
    )
