import datetime
import pendulum
from airflow.utils.dates import days_ago
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}


@dag(
    dag_id="postgres_orders_popular_day_of_week",
    schedule_interval='@once',
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)
def InsertDataDag():
    PostgresOperator(
        task_id="new_room_booking",
        postgres_conn_id='food_delivery_db',
        sql="""
            DELETE FROM public."Employees";
            INSERT INTO public."Employees" VALUES (1, 'TALES OF SHIVA', 'Mark', 'mark', 0);
            """
    )    


dag = InsertDataDag()
