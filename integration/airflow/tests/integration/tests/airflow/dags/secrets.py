# SPDX-License-Identifier: Apache-2.0
import logging

from airflow.models import BaseOperator
from airflow.utils.dates import days_ago
from airflow.utils.log.secrets_masker import mask_secret

from openlineage.client import set_producer

set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")

from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version
if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):
    from openlineage.airflow import DAG
else:
    from airflow import DAG


"""This shouldn't have extractor - we're testing if we'll see password in UnknownSourceAttribute"""
class SecretsOperator(BaseOperator):
    template_fields = ['password']

    def __init__(self, password, **kwargs):
        super().__init__(**kwargs)
        self.password = password

    def execute(self, context):
        mask_secret(self.password)
        logging.getLogger(__name__).warning(f"Password! {self.password}")


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}


dag = DAG(
    'secrets',
    schedule_interval='@once',
    default_args=default_args,
    description='Secrets test'
)


t1 = SecretsOperator(
    task_id='secrets',
    password="{{ var.value.secrets_password }}",
    dag=dag,
)
