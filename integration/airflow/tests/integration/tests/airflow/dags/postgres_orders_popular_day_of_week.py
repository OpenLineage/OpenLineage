# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from airflow import DAG
from airflow.version import version as AIRFLOW_VERSION
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from openlineage.client import set_producer
from pkg_resources import parse_version
set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")


def get_sql() -> str:
    return '''
    CREATE TABLE IF NOT EXISTS popular_orders_day_of_week (
      order_day_of_week VARCHAR(64) NOT NULL,
      order_placed_on   TIMESTAMP NOT NULL,
      orders_placed     INTEGER NOT NULL
    );'''


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}




dag = DAG(
    'postgres_orders_popular_day_of_week',
    schedule_interval='@once',
    default_args=default_args,
    user_defined_macros={
        "get_sql": get_sql
    },
    description='Determines the popular day of week orders are placed.'
)

if parse_version(AIRFLOW_VERSION) < parse_version('2.5.0'):
    t1 = PostgresOperator(    
        task_id='postgres_if_not_exists',
        postgres_conn_id='food_delivery_db',
        sql="{{ get_sql() }}",
        dag=dag
    )
else:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    t1 = SQLExecuteQueryOperator(
        task_id='postgres_if_not_exists',
        conn_id='food_delivery_db',
        sql="{{ get_sql() }}",
        dag=dag
    )

t2 = PostgresOperator(
    task_id='postgres_insert',
    postgres_conn_id='food_delivery_db',
    sql='''
    INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)
    SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
           order_placed_on,
           COUNT(*) AS orders_placed
      FROM top_delivery_times
     GROUP BY order_placed_on
    ''',
    dag=dag
)

t1 >> t2
