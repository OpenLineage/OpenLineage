# SPDX-License-Identifier: Apache-2.0

import time

from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

from openlineage.client import set_producer
set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")

from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version
if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):
    from openlineage.airflow import DAG
else:
    from airflow import DAG


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

dag = DAG(
    'bigquery_orders_popular_day_of_week',
    schedule_interval='@once',
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)

PROJECT_ID = 'openlineage-ci'
DATASET_ID = 'airflow_integration'
CONNECTION = 'bq_conn'

t1 = BigQueryOperator(
    task_id='bigquery_if_not_exists',
    bigquery_conn_id=CONNECTION,
    sql=f'''
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.popular_orders_day_of_week` (
      order_day_of_week INTEGER NOT NULL,
      order_placed_on   TIMESTAMP NOT NULL,
      orders_placed     INTEGER NOT NULL
    );''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='bigquery_empty_table',
    bigquery_conn_id='bq_conn',
    sql=f'''
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.top_delivery_times` (
      order_placed_on     TIMESTAMP NOT NULL,
    );''',
    use_legacy_sql=False,
    dag=dag
)

delay_1 = PythonOperator(
    task_id="delay_python_task_1",
    dag=dag,
    python_callable=lambda: time.sleep(10)
)

t3 = BigQueryOperator(
    task_id='bigquery_seed',
    bigquery_conn_id='bq_conn',
    sql=f'''
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.top_delivery_times` (order_placed_on) VALUES
    (TIMESTAMP('2008-12-25 15:30:00+00')),
    (TIMESTAMP('2008-12-25 15:32:00+00')),
    (TIMESTAMP('2008-12-26 15:30:00+00'))''',
    use_legacy_sql=False,
    dag=dag
)

t4 = BigQueryOperator(
    task_id='bigquery_insert',
    bigquery_conn_id='bq_conn',
    sql=f'''
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.popular_orders_day_of_week` (order_day_of_week, order_placed_on, orders_placed)
    SELECT EXTRACT(DAYOFWEEK FROM order_placed_on) AS order_day_of_week,
        order_placed_on,
        COUNT(*) AS orders_placed
    FROM airflow_integration.top_delivery_times
    GROUP BY order_placed_on;''',
    use_legacy_sql=False,
    dag=dag
)

t5 = BigQueryOperator(
    task_id='bigquery_truncate_top_delivery_times',
    bigquery_conn_id='bq_conn',
    sql=f'''
    TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.top_delivery_times`;''',
    use_legacy_sql=False,
    dag=dag
)

t6 = BigQueryOperator(
    task_id='bigquery_truncate_popular_orders_day_of_week',
    bigquery_conn_id='bq_conn',
    sql=f'''
    TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.popular_orders_day_of_week`;''',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2 >> delay_1 >> t3 >> t4 >> t5 >> t6
