import datetime
import os
import uuid

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from openlineage.client import set_producer, OpenLineageClient
from openlineage.client.run import RunEvent, Run, Job, RunState


_PRODUCER="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow"

set_producer(_PRODUCER)

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
    'event_order',
    schedule_interval='@once',
    default_args=default_args,
    description='Test dag.'
)


def emit_event():
    client = OpenLineageClient.from_environment()
    client.emit(RunEvent(
        RunState.COMPLETE,
        datetime.datetime.now().isoformat(),
        Run(runId=str(uuid.uuid4())),
        Job(namespace=os.getenv('OPENLINEAGE_NAMESPACE'), name='emit_event.wait-for-me'),
        _PRODUCER,
        [],
        []
    ))


t1 = BashOperator(
    task_id='just_wait',
    bash_command="sleep 5",
    dag=dag
)

t2 = PythonOperator(
    task_id='emit_event',
    python_callable=emit_event,
    dag=dag
)

t1 >> t2
