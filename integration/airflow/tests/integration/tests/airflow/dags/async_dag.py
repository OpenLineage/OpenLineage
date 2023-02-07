# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from airflow.decorators import dag
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from airflow.utils.dates import days_ago


class TimeDeltaCustomAsync(TimeDeltaSensorAsync):
    def execute_complete(self, context, event=None):
        print(context, event)
        # fail on purpose
        raise Exception

@dag(
    schedule_interval="@once",
    start_date=days_ago(7),
    catchup=False,
)
def async_dag():
    working_operator = TimeDeltaSensorAsync(task_id='timedelta_sensor', delta=timedelta(seconds=10))
    failing_operator = TimeDeltaCustomAsync(task_id='failing_sensor', delta=timedelta(seconds=10))
    working_operator >> failing_operator
    

async_dag = async_dag()
