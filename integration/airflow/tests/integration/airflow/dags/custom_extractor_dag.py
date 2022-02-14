import os
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.dates import days_ago
from openlineage.client import set_producer

os.environ["OPENLINEAGE_EXTRACTOR_CustomOperator"] = 'custom_extractor.CustomExtractor'
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
