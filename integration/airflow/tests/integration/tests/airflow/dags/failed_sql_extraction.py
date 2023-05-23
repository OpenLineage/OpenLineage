# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from openlineage.client import set_producer
from packaging.version import parse as parse_version

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.version import version as AIRFLOW_VERSION

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
    'failed_sql_extraction',
    schedule_interval='@once',
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)


# Some sql-parser unsupported syntax
sql = "CREATE TYPE myrowtype AS (f1 int, f2 text, f3 numeric)"


if parse_version(AIRFLOW_VERSION) < parse_version('2.5.0'):
    t1 = PostgresOperator(
        task_id='fail',
        postgres_conn_id='food_delivery_db',
        sql=sql,
        dag=dag
    )
else:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    t1 = SQLExecuteQueryOperator(
        task_id='fail',
        conn_id='food_delivery_db',
        sql=sql,
        dag=dag
    )
