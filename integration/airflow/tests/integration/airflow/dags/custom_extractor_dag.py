import os

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

os.environ["OPENLINEAGE_EXTRACTOR_BashOperator"] = 'custom_extractor.BashExtractor'
from openlineage.airflow import DAG
from openlineage.client import set_producer

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

t1 = BashOperator(
    task_id='custom_extractor',
    bash_command="ls -halt",
    dag=dag
)
