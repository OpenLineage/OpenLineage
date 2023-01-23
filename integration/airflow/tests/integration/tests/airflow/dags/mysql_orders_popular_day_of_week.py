# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client import set_producer

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
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
    'mysql_orders_popular_day_of_week',
    schedule_interval='@once',
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)


t1 = MySqlOperator(
    task_id='mysql_if_not_exists',
    mysql_conn_id='mysql_conn',
    sql='''
    CREATE TABLE IF NOT EXISTS popular_orders_day_of_week (
      order_day_of_week VARCHAR(64) NOT NULL,
      order_placed_on   TIMESTAMP NOT NULL,
      orders_placed     INTEGER NOT NULL
    );''',
    dag=dag
)

t2 = MySqlOperator(
    task_id='mysql_insert',
    mysql_conn_id='mysql_conn',
    sql='''
    INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)
    SELECT WEEKDAY(order_placed_on) AS order_day_of_week,
           order_placed_on,
           COUNT(*) AS orders_placed
      FROM top_delivery_times
     GROUP BY order_placed_on
    ''',
    dag=dag
)

t1 >> t2
