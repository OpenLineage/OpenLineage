from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

from airflow.utils.dates import days_ago
from openlineage.client import set_producer
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

data_context_dir = "/opt/data/great_expectations"

dag = DAG(
    'great_expectations_validation',
    schedule_interval='@once',
    default_args=default_args,
    description='Validates data.'
)

t1 = GreatExpectationsOperator(
    task_id='ge_sqlite_test',
    run_name="ge_sqlite_run",
    checkpoint_name="sqlite",
    data_context_root_dir=data_context_dir,
    dag=dag,
    fail_task_on_validation_failure=False,
    validation_operator_name="ol_operator",
    do_xcom_push=False
)

t2 = GreatExpectationsOperator(
    task_id='ge_pandas_test',
    run_name="ge_pandas_run",
    checkpoint_name="pandas",
    data_context_root_dir=data_context_dir,
    dag=dag,
    fail_task_on_validation_failure=False,
    validation_operator_name="ol_operator",
    do_xcom_push=False
)

t3 = GreatExpectationsOperator(
    task_id='ge_bad_sqlite_test',
    run_name="ge_bad_sqlite_run",
    checkpoint_name="bad_sqlite",
    data_context_root_dir=data_context_dir,
    dag=dag,
    fail_task_on_validation_failure=False,
    validation_operator_name="ol_operator",
    do_xcom_push=False
)

t1 >> t2 >> t3
