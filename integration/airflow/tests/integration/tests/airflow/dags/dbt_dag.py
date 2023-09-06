# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.version import version as AIRFLOW_VERSION

PLUGIN_MACRO = "{{ macros.OpenLineagePlugin.lineage_parent_id(run_id, task, task_instance) }}"


PROJECT_DIR = "/opt/data/dbt/testproject"
PROFILE_DIR = "/opt/data/dbt/profiles"

default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

dag = DAG(
    'dbt',
    schedule_interval='@once',
    default_args=default_args,
    description='Runs dbt model build.',
)

t1 = BashOperator(
    task_id='dbt_seed',
    dag=dag,
    bash_command=f"source /opt/airflow/dbt_venv/bin/activate && dbt seed --full-refresh --project-dir={PROJECT_DIR} --profiles-dir={PROFILE_DIR} && deactivate",  # noqa: E501
    env={
        **os.environ,
        "OPENLINEAGE_PARENT_ID": PLUGIN_MACRO,
        "DBT_PROFILE": "bigquery"
    }
)

t2 = BashOperator(
    task_id='dbt_run',
    dag=dag,
    bash_command=f"source /opt/airflow/dbt_venv/bin/activate && dbt-ol run --project-dir={PROJECT_DIR} --profiles-dir={PROFILE_DIR} && deactivate",  # noqa: E501
    env={
        **os.environ,
        "OPENLINEAGE_PARENT_ID": PLUGIN_MACRO,
        "DBT_PROFILE": "bigquery"
    }
)

t1 >> t2

if AIRFLOW_VERSION == os.environ.get("SNOWFLAKE_AIRFLOW_TEST_VERSION", "2.3.4"):
    t3 = BashOperator(
        task_id='dbt_seed_snowflake',
        dag=dag,
        bash_command=f"source /opt/airflow/dbt_venv/bin/activate && dbt seed --full-refresh --project-dir={PROJECT_DIR} --profiles-dir={PROFILE_DIR} && deactivate",  # noqa: E501
        env={
            **os.environ,
            "OPENLINEAGE_PARENT_ID": PLUGIN_MACRO,
            "DBT_PROFILE": "snowflake"
        }
    )

    t4 = BashOperator(
        task_id='dbt_run_snowflake',
        dag=dag,
        bash_command=f"source /opt/airflow/dbt_venv/bin/activate && dbt-ol run --project-dir={PROJECT_DIR} --profiles-dir={PROFILE_DIR}",  # noqa: E501
        env={
            **os.environ,
            "OPENLINEAGE_PARENT_ID": PLUGIN_MACRO,
            "DBT_PROFILE": "snowflake"
        }
    )

    t2 >> t3 >> t4
