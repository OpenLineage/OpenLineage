# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from openlineage.client import set_producer

from airflow import DAG
from airflow.utils.dates import days_ago

set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")


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
    checkpoint_kwargs={"run_name": "ge_sqlite_run"},
    do_xcom_push=False
)

t2 = GreatExpectationsOperator(
    task_id='ge_pandas_test',
    run_name="ge_pandas_run",
    checkpoint_name="pandas",
    data_context_root_dir=data_context_dir,
    dag=dag,
    fail_task_on_validation_failure=False,
    checkpoint_kwargs={"run_name": "ge_pandas_run"},
    do_xcom_push=False
)

t3 = GreatExpectationsOperator(
    task_id='ge_bad_sqlite_test',
    run_name="ge_bad_sqlite_run",
    checkpoint_name="bad_sqlite",
    data_context_root_dir=data_context_dir,
    dag=dag,
    fail_task_on_validation_failure=False,
    checkpoint_kwargs={"run_name": "ge_bad_sqlite_run"},
    do_xcom_push=False
)

t1 >> t2 >> t3
