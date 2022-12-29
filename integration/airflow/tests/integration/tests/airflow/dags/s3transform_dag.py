# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.utils.dates import days_ago


with DAG(dag_id="s3transform_dag", start_date=days_ago(7), schedule_interval="@once") as dag:
    S3FileTransformOperator(
        task_id="s3transform_task",
        source_aws_conn_id="aws_local_conn",
        dest_aws_conn_id="aws_local_conn",
        source_s3_key="s3://testbucket/testfile",
        dest_s3_key="s3://testbucket/testfile2",
        transform_script="cp",
        replace=True,
    )
