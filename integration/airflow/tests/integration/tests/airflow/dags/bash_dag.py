# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

dag = DAG(dag_id="bash_dag", start_date=days_ago(7), schedule_interval="@once")

failing_task = BashOperator(dag=dag, task_id="failing_task", bash_command="exit 1;")

success_task = BashOperator(dag=dag, task_id="success_task", bash_command="exit 0;")
