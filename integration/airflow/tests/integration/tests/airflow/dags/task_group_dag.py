# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from airflow.decorators import dag, task, task_group

from airflow.utils.dates import days_ago


@dag(
    schedule_interval="@once",
    start_date=days_ago(7),
    catchup=False,
)
def task_group_dag():
    @task(task_id="extract", retries=2)
    def extract_data():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task()
    def transform_sum(order_data_dict: dict):
        total_order_value = 0
        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def transform_avg(order_data_dict: dict):
        total_order_value = 0
        for value in order_data_dict.values():
            total_order_value += value
            avg_order_value = total_order_value / len(order_data_dict)

        return {"avg_order_value": avg_order_value}

    @task_group(tooltip="Transform tooltip")
    def transform_values(order_data_dict):
        return {
            "avg": transform_avg(order_data_dict),
            "total": transform_sum(order_data_dict),
        }

    @task()
    def load(order_values: dict):
        print(order_values)

    load(transform_values(extract_data()))


task_group_dag = task_group_dag()
