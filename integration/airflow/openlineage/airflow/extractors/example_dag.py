# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='test_dummy_dag',
    description='Test dummy DAG',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2020, 1, 8),
    catchup=False,
    max_active_runs=1
)


python_task_getcwd = PythonOperator(task_id="python-task", python_callable=os.getcwd, dag=dag)

bash_task = BashOperator(task_id="bash-task", bash_command="ls -halt && exit 0", dag=dag)

python_task_getcwd >> bash_task
