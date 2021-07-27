# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from marquez_airflow import DAG

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
