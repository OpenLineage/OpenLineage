# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from openlineage.client import set_producer


_PRODUCER="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow"
set_producer(_PRODUCER)


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}


dag = DAG(
    'dag_event_order',
    schedule_interval='@once',
    default_args=default_args,
    description='Test dag.'
)


def callable():
    print(10)


python_task = PythonOperator(task_id="first_task", python_callable=callable, dag=dag)
