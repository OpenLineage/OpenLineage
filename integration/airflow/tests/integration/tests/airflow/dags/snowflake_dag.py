# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client import set_producer

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

dag = DAG(
    'snowflake',
    schedule_interval='@once',
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)

CONNECTION = "snowflake_conn"

t1 = SnowflakeOperator(
    task_id='snowflake_if_not_exists',
    snowflake_conn_id=CONNECTION,
    sql='''
    CREATE TABLE IF NOT EXISTS test_orders (
      ord   NUMBER,
      str   STRING,
      num   NUMBER
    );''',
    dag=dag
)

t2 = SnowflakeOperator(
    task_id='snowflake_insert',
    snowflake_conn_id=CONNECTION,
    sql='''
    INSERT INTO test_orders (ord, str, num) VALUES
    (1, 'b', 15),
    (2, 'a', 21),
    (3, 'b', 7);
    ''',
    dag=dag
)

t3 = SnowflakeOperator(
    task_id='snowflake_truncate',
    snowflake_conn_id=CONNECTION,
    sql="TRUNCATE TABLE test_orders;",
    dag=dag
)

t1 >> t2 >> t3
