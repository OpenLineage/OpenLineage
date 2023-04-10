# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

from openlineage.airflow.extractors.s3_snowflake_extractor import S3ToSnowflakeExtractor
from openlineage.client.run import Dataset as InputDataset
from openlineage.common.dataset import Dataset, Field, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta

from airflow.models import DAG, Connection
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import days_ago

CONN_ID = 'food_delivery_db'
CONN_URI = 'snowflake://snowflake.example/db-schema?account=test_account&database=FOOD_DELIVERY&region=us-east&warehouse=snow-warehouse&secret=hideit'  # noqa
CONN_URI_WITH_EXTRA_PREFIX = 'snowflake://snowflake.example/db-schema?extra__snowflake__account=test_account&extra__snowflake__database=FOOD_DELIVERY&extra__snowflake__region=us-east&extra__snowflake__warehouse=snow-warehouse&extra__snowflake__secret=hideit'  # noqa
CONN_URI_URIPARSED = 'snowflake://snowflake.example/db-schema?account=%5B%27test_account%27%5D&database=%5B%27FOOD_DELIVERY%27%5D&region=%5B%27us-east%27%5D&warehouse=%5B%27snow-warehouse%27%5D'  # noqa

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


def mock_get_table():
    mocked = mock.MagicMock()
    columns = [
        DbColumn(
            name='col1',
            type='dt1',
            ordinal_position=1,
        )
    ]

    mocked.return_value = DbTableSchema(
        schema_name='schema',
        table_name='table',
        columns=columns,
    )

    return mocked.return_value


def mock_get_table_schemas():
    mocked = mock.MagicMock()
    columns = [
        DbColumn(
            name='col1',
            type='dt1',
            ordinal_position=1,
        )
    ]
    mocked.return_value = [DbTableSchema(
        schema_name='schema',
        table_name='table',
        columns=columns,
    )]

    return mocked.return_value


@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_extract_on_complete(get_connection):
    source = Source(
        scheme='snowflake',
        authority='test_account',
        connection_url=CONN_URI
    )

    expected_inputs = [
        InputDataset(
            namespace="snowflake_stage_MY_S3_STAGE",
            name="my_api_data.json",
            facets={}
        )
    ]

    mock_get_table()
    mock_get_table_schemas()

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

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

    task_metadata = S3ToSnowflakeExtractor(TASK).extract_on_complete()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == expected_outputs
