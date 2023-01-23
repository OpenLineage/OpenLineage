# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
from typing import Any

from openlineage.client import set_producer

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.dates import days_ago

os.environ["OPENLINEAGE_EXTRACTOR_CustomOperator"] = 'custom_extractor.CustomExtractor'
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
    'custom_extractor',
    schedule_interval='@once',
    default_args=default_args,
    description='Test dag.'
)


class CustomOperator(BaseOperator):
    def execute(self, context: Any):
        for i in range(10):
            print(i)


t1 = CustomOperator(
    task_id='custom_extractor',
    dag=dag
)
