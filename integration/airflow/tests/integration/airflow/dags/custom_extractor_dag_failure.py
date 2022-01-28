import os

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from openlineage.client import set_producer

os.environ["OPENLINEAGE_EXTRACTOR_BashOperator"] = 'custom_extractor.BashExtractor'
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
    'custom_extractor_fail',
    schedule_interval='@once',
    default_args=default_args,
    description='Test dag.'
)

t1 = BashOperator(
    task_id='custom_extractor_fail',
    bash_command="ls -halt && sleep 1 && exit 1",
    dag=dag
)
