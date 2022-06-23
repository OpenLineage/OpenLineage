# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from openlineage.client import set_producer


_PRODUCER="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow"

set_producer(_PRODUCER)

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


# test_dag has schedule_interval none, to be triggered by API request
dag = DAG(
    'test_dag',
    schedule_interval=None,
    default_args=default_args,
    description='Test dag.'
)


def callable():
    print(10)


python_task = PythonOperator(task_id="python_task", python_callable=callable, dag=dag)

bash_task = BashOperator(task_id="bash_task", bash_command="ls -halt && exit 0", dag=dag)

python_task >> bash_task
