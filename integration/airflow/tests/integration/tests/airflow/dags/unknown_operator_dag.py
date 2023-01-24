# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults


class TestUnknownDummyOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: Any):
        for i in range(10):
            print(i)


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}
dag = DAG(
    'unknown_operator_dag',
    schedule_interval='@once',
    default_args=default_args,
    description='Test unknown_operator dag.'
)


t1 = TestUnknownDummyOperator(
    task_id='unknown_operator',
    dag=dag
)
