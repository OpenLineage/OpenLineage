# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.utils.dates import days_ago

from openlineage.client import set_producer
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
    'trino_orders_popular_day_of_week',
    schedule_interval='@once',
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)


t1 = TrinoOperator(
    task_id='trino_create',
    trino_conn_id='trino_conn',
    sql='CREATE SCHEMA IF NOT EXISTS memory.default',
    dag=dag
)


t2 = TrinoOperator(
    task_id='trino_insert',
    trino_conn_id='trino_conn',
    sql='''
    CREATE TABLE memory.default.popular_orders_day_of_week AS
    SELECT DAY_OF_WEEK(order_placed_on) AS order_day_of_week,
           order_placed_on,
           COUNT(*) AS orders_placed
      FROM postgresql.public.top_delivery_times
     GROUP BY order_placed_on
    ''',
    dag=dag
)


t1 >> t2
