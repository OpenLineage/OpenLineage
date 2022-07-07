# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

REDSHIFT_DATABASE = getenv("REDSHIFT_DATABASE", "dev")
REDSHIFT_DATABASE_USER = getenv("REDSHIFT_DATABASE_USER", "admin")

REDSHIFT_QUERY = """
CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
            """
POLL_INTERVAL = 10


@task(task_id="output_results")
def output_query_results(statement_id):
    hook = RedshiftDataHook(region_name="eu-west-2")
    resp = hook.conn.get_statement_result(
        Id=statement_id,
    )

    print(resp)
    return resp


with DAG(
    dag_id="redshiftter_dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_redshift_data]
    task_query = RedshiftDataOperator(
        # aws_conn_id="redshit", 
        task_id='redshift_query',
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_DATABASE_USER,
        cluster_identifier="redshift-cluster-1",
        sql=REDSHIFT_QUERY,
        poll_interval=POLL_INTERVAL,
        await_result=True,
        region='eu-west-2',
    )
    # [END howto_operator_redshift_data]

    task_output = output_query_results(task_query.output)
