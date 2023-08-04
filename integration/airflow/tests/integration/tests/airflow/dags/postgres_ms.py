from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
        'owner': 'someone',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': False,
        'email_on_retry': False,
        'email': ['someone@gmail.com']
}

dag = DAG(
        'postgres_ms',
        schedule_interval='@once',
        catchup=False,
        is_paused_upon_creation=False,
        default_args=default_args,
        description='DAG that copies table by insert and select'
)

t1 = PostgresOperator(
        task_id='init_user',
        postgres_conn_id='food_delivery_db',
        sql='''
        DROP TABLE IF EXISTS users;
        CREATE TABLE IF NOT EXISTS users (
                name VARCHAR NOT NULL,
                age INTEGER
        );
        INSERT INTO users(name, age)
        VALUES
                ('deer', 22),
                ('ruihua', 24),
                ('kathy', 5),
                ('john', 15);
        ''',
        dag=dag
)

t2 = PostgresOperator(
        task_id='init_new_user',
        postgres_conn_id='food_delivery_db',
        sql='''
        CREATE TABLE IF NOT EXISTS new_users (
                name VARCHAR NOT NULL,
                age INTEGER
        );
        ''',
        dag=dag
)

t3 = PostgresOperator(
        task_id='copy_from_user_to_new_users',
        postgres_conn_id='food_delivery_db',
        sql='''
        INSERT INTO new_users(name, age)
        SELECT * FROM users;
        ''',
        dag=dag
)


t1 >> t2 >> t3
