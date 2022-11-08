# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import os

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from openlineage.airflow.utils import JobIdMapping

try:
    from airflow.utils.db import create_session
except ImportError:
    from airflow.utils.session import create_session

# do not need airflow integration
from airflow import DAG
from airflow.version import version as AIRFLOW_VERSION


def lineage_parent_id(run_id, task):
    with create_session() as session:
        job_name = f"{task.dag_id}.{task.task_id}"
        ids = str(JobIdMapping.get(job_name, run_id, session))
        return f"{os.getenv('OPENLINEAGE_NAMESPACE')}/{job_name}/{ids}"


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

if AIRFLOW_VERSION == os.environ.get("SNOWFLAKE_AIRFLOW_TEST_VERSION", "2.3.4"):
    dag = DAG(
        'dbt_snowflake',
        schedule_interval='@once',
        default_args=default_args,
        description='Runs dbt model build.',
        user_defined_macros={
            "lineage_parent_id": lineage_parent_id,
        }
    )

    t1 = BashOperator(
        task_id='dbt_seed',
        dag=dag,
        bash_command=f"dbt seed --full-refresh --project-dir={PROJECT_DIR} --profiles-dir={PROFILE_DIR}",
        env={
            **os.environ,
            "OPENLINEAGE_PARENT_ID": "{{ lineage_parent_id(run_id, task) }}",
            "DBT_PROFILE": "snowflake"
        }
    )

    t2 = BashOperator(
        task_id='dbt_run',
        dag=dag,
        bash_command=f"dbt-ol run --project-dir={PROJECT_DIR} --profiles-dir={PROFILE_DIR}",
        env={
            **os.environ,
            "OPENLINEAGE_PARENT_ID": "{{ lineage_parent_id(run_id, task) }}",
            "DBT_PROFILE": "snowflake"
        }
    )

    t1 >> t2
