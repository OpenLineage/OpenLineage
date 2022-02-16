# SPDX-License-Identifier: Apache-2.0

from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago

from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version
if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):
    from openlineage.airflow import DAG
else:
    from airflow import DAG


class TestUnknownDummyOperator(DummyOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(TestUnknownDummyOperator, self).__init__(*args, **kwargs)


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
