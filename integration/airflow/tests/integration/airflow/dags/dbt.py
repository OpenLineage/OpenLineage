from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# do not need airflow integration
from airflow import DAG

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
    'dbt_dag',
    schedule_interval='@once',
    default_args=default_args,
    description='Runs dbt model build.'
)

t1 = BashOperator(
    task_id='dbt_seed',
    dag=dag,
    bash_command=f"dbt seed --full-refresh --project-dir={PROJECT_DIR} --profiles-dir={PROFILE_DIR}"
)

t2 = BashOperator(
    task_id='dbt_run',
    dag=dag,
    bash_command=f"dbt-ol run --project-dir={PROJECT_DIR} --profiles-dir={PROFILE_DIR}"
)

t1 >> t2
