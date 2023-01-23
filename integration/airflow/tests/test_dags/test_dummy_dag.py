# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

log = logging.getLogger(__name__)

dag = DAG(dag_id='test_dummy_dag',
          description='Test dummy DAG',
          schedule_interval='*/2 * * * *',
          start_date=datetime(2020, 1, 8),
          catchup=False,
          max_active_runs=1)
log.debug("dag created.")

dummy_task = DummyOperator(
    task_id='test_dummy',
    dag=dag
)
