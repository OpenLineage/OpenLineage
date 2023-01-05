# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="mapped_dag", start_date=datetime(2020, 4, 7), schedule_interval="@once"
) as dag:
    input = PythonOperator(
        task_id="get_input",
        python_callable=lambda: list(map(lambda x: "echo " + str(x), [1, 2, 3])),
    )

    counts = BashOperator.partial(task_id="multiply", do_xcom_push=True).expand(
        bash_command=XComArg(input),
    )

    @task
    def total(numbers):
        return sum((int(x) for x in numbers))

    total(numbers=XComArg(counts))
