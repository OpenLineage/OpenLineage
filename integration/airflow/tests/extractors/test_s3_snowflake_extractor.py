# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import random
from datetime import datetime
from unittest import mock

import pytz
from openlineage.airflow.extractors.s3_snowflake_extractor import S3ToSnowflakeOperatorExtractor
from openlineage.client.run import Dataset as InputDataset
from openlineage.common.dataset import Dataset, Field, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta
from pkg_resources import parse_version

from airflow.models import DAG, TaskInstance
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.version import version as AIRFLOW_VERSION

CONN_ID = 'food_delivery_db'
CONN_URI = 'snowflake://snowflake:example/db-schema?account=test_account&database=FOOD_DELIVERY&region=us-east&warehouse=snow-warehouse&secret=hideit'  # noqa
CONN_URI_WITH_EXTRA_PREFIX = 'snowflake://snowflake:example/db-schema?extra__snowflake__account=test_account&extra__snowflake__database=FOOD_DELIVERY&extra__snowflake__region=us-east&extra__snowflake__warehouse=snow-warehouse&extra__snowflake__secret=hideit'  # noqa
CONN_URI_URIPARSED = 'snowflake://snowflake:example/db-schema?account=%5B%27test_account%27%5D&database=%5B%27FOOD_DELIVERY%27%5D&region=%5B%27us-east%27%5D&warehouse=%5B%27snow-warehouse%27%5D'  # noqa

DB_NAME = 'FOOD_DELIVERY'
DB_SCHEMA_NAME = 'PUBLIC'
DB_TABLE_NAME = DbTableMeta('DISCOUNTS')
DB_TABLE_COLUMNS = [
    DbColumn(
        name='ID',
        type='int4',
        ordinal_position=1
    ),
    DbColumn(
        name='AMOUNT_OFF',
        type='int4',
        ordinal_position=2
    ),
    DbColumn(
        name='CUSTOMER_EMAIL',
        type='varchar',
        ordinal_position=3
    ),
    DbColumn(
        name='STARTS_ON',
        type='timestamp',
        ordinal_position=4
    ),
    DbColumn(
        name='ENDS_ON',
        type='timestamp',
        ordinal_position=5
    )
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME,
    table_name=DB_TABLE_NAME,
    columns=DB_TABLE_COLUMNS
)
NO_DB_TABLE_SCHEMA = []

SQL = f"SELECT * FROM {DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name};"

DAG_ID = 'email_discounts'
DAG_OWNER = 'datascience'
DAG_DEFAULT_ARGS = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}
DAG_DESCRIPTION = 'Email discounts to customers that have experienced order delays daily'

DAG = dag = DAG(
    DAG_ID,
    schedule_interval='@weekly',
    default_args=DAG_DEFAULT_ARGS,
    description=DAG_DESCRIPTION
)

TASK_ID = 'select'
TASK = S3ToSnowflakeOperator(
    task_id=TASK_ID,
    s3_keys=['my_api_data.json'],
    table='DISCOUNTS',
    schema=DB_SCHEMA_NAME,
    stage='MY_S3_STAGE',
    file_format="(type = 'json')",
    snowflake_conn_id=CONN_ID,
    dag=DAG
)


def mock_get_hook(operator):
    mocked = mock.MagicMock()
    mocked.return_value.conn_name_attr = 'snowflake_conn_id'
    mocked.return_value.get_uri.return_value = "snowflake://user:pass@xy123456/db/schema"
    if hasattr(operator, 'get_db_hook'):
        operator.get_db_hook = mocked
    else:
        operator.get_hook = mocked


def get_hook_method(operator):
    if hasattr(operator, 'get_db_hook'):
        return operator.get_db_hook
    else:
        return operator.get_hook

def get_ti(task):
    kwargs = {}
    if parse_version(AIRFLOW_VERSION) > parse_version("2.2.0"):
        kwargs['run_id'] = 'test_run_id'  # change in 2.2.0
    task_instance = TaskInstance(
        task=task,
        execution_date=datetime.utcnow().replace(tzinfo=pytz.utc),
        state=State.SUCCESS,
        **kwargs)
    task_instance.job_id = random.randrange(10000)

    return task_instance

@mock.patch('openlineage.airflow.extractors.sql_extractor.get_table_schemas')  # noqa
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
@mock.patch('openlineage.airflow.extractors.snowflake_extractor.execute_query_on_hook')
def test_extract_on_complete(execute_query_on_hook, get_connection, mock_get_table_schemas):
    source = Source(
        scheme='snowflake',
        authority='test_account',
        connection_url=CONN_URI_URIPARSED
    )

    expected_inputs = [
        InputDataset(
            namespace="s3://source-bucket",
            name="s3://source-bucket/path/to/source_file.csv",
            facets={}
        )
    ]

    mock_get_table_schemas.return_value = (
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME)],
        [],
    )

    # conn = Connection()
    # conn.parse_from_uri(uri=CONN_URI)
    # get_connection.return_value = conn

    mock_get_hook(TASK)

    get_hook_method(TASK).return_value._get_conn_params.return_value = {
        'account': 'test_account',
        'database': DB_NAME
    }

    expected_outputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=source,
            fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS],
        ).to_openlineage_dataset()
    ]

    task_instance = get_ti(TASK)
    task_metadata = S3ToSnowflakeOperatorExtractor(TASK).extract_on_complete(task_instance)

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == expected_outputs
