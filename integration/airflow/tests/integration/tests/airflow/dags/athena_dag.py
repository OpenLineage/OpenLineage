# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from urllib.parse import urlparse

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

from openlineage.airflow.utils import try_import_from_string
from openlineage.client import set_producer
set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")

AthenaOperator = try_import_from_string(
    "airflow.providers.amazon.aws.operators.athena.AthenaOperator"
)
if AthenaOperator is None:
    AthenaOperator = try_import_from_string(
        "airflow.providers.amazon.aws.operators.athena.AWSAthenaOperator"
    )

default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

dag = DAG(
    'athena_dag',
    schedule_interval='@once',
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)

CONNECTION = "aws_conn"
AWS_ATHENA_SUFFIX = os.environ["AWS_ATHENA_SUFFIX"]
DATABASE = f"openlineage_integration_{AWS_ATHENA_SUFFIX}"
OUTPUT_LOCATION = os.environ["AWS_ATHENA_OUTPUT_LOCATION"] + AWS_ATHENA_SUFFIX
parsed = urlparse(OUTPUT_LOCATION)
S3_BUCKET = parsed.netloc
S3_PREFIX = parsed.path

t0 = AthenaOperator(
    task_id='athena_create_database',
    aws_conn_id=CONNECTION,
    query=f'CREATE DATABASE IF NOT EXISTS {DATABASE};',
    database=DATABASE,
    output_location=OUTPUT_LOCATION,
    max_tries=4,
    dag=dag
)

t1 = AthenaOperator(
    task_id='athena_create_table',
    aws_conn_id=CONNECTION,
    query=f'''
    CREATE EXTERNAL TABLE IF NOT EXISTS test_orders (
      ord   INT,
      str   STRING,
      num   INT
    )
    LOCATION '{OUTPUT_LOCATION}';
    ''',
    database=DATABASE,
    output_location=OUTPUT_LOCATION,
    max_tries=4,
    dag=dag
)

t2 = AthenaOperator(
    task_id='athena_insert',
    aws_conn_id=CONNECTION,
    query='''
    INSERT INTO test_orders (ord, str, num) VALUES
    (1, 'b', 15),
    (2, 'a', 21),
    (3, 'b', 7);
    ''',
    database=DATABASE,
    output_location=OUTPUT_LOCATION,
    max_tries=4,
    dag=dag
)

t3 = AthenaOperator(
    task_id='athena_drop_table',
    aws_conn_id=CONNECTION,
    query="DROP TABLE test_orders;",
    database=DATABASE,
    output_location=OUTPUT_LOCATION,
    max_tries=4,
    dag=dag
)


def delete_objects():
    hook = S3Hook(aws_conn_id=CONNECTION)
    keys = hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
    hook.delete_objects(bucket=S3_BUCKET, keys=keys)


t4 = PythonOperator(
    task_id="delete_s3_objects",
    python_callable=delete_objects,
    dag=dag
)

t0 >> t1 >> t2 >> t3 >> t4
