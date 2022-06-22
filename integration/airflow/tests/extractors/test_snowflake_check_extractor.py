# SPDX-License-Identifier: Apache-2.0.

from unittest import mock

import pytest
if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
    from airflow.providers.snowflake.operators.snowflake_operator import SnowflakeCheckOperator
from airflow.models import Connection
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version

from openlineage.common.models import (
        DbTableSchema,
        DbColumn
    )
from openlineage.common.sql import DbTableMeta
from openlineage.common.dataset import Source, Dataset
from openlineage.airflow.extractors.snowflake_check_extractor import SnowflakeCheckExtractor

CONN_ID = 'food_delivery_db'
CONN_URI = 'snowflake://localhost:5432/food_delivery'

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

SQL = f"SELECT IF COUNT(*) == 10 THEN 0 ELSE 1 END FROM {DB_NAME}.{DB_TABLE_NAME.name};"

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

TASK_ID = 'row_count_check'
TASK = SnowflakeCheckOperator(
        task_id=TASK_ID,
        snowflake_conn_id=CONN_ID,
        sql=SQL,
        dag=DAG
    )


@mock.patch('openlineage.airflow.extractors.snowflake_extractor.SnowflakeExtractor._get_table_schemas')  # noqa
@mock.patch('openlineage.airflow.extractors.postgres_extractor.get_connection')
def test_extract_on_complete(get_connection, mock_get_table_schemas):
    mock_get_table_schemas.side_effect = \
        [[DB_TABLE_SCHEMA], NO_DB_TABLE_SCHEMA]

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    TASK.get_hook = mock.MagicMock()
    TASK.get_hook.return_value._get_conn_params.return_value = {
        'account': 'test_account',
        'database': DB_NAME
    }

    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=Source(
                scheme='snowflake',
                authority='test_account',
                connection_url=CONN_URI
            ),
            fields=[]
        ).to_openlineage_dataset()]

    task_metadata = SnowflakeCheckExtractor(TASK).extract_on_complete()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


@pytest.mark.skipif(parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"), reason="Airflow 2+ test")  # noqa
@mock.patch('openlineage.airflow.extractors.snowflake_extractor.SnowflakeExtractor._get_table_schemas')  # noqa
@mock.patch('openlineage.airflow.extractors.postgres_extractor.get_connection')
def test_extract_query_ids(get_connection, mock_get_table_schemas):
    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    TASK.get_hook = mock.MagicMock()
    TASK.query_ids = ["1500100900"]

    task_metadata = SnowflakeCheckExtractor(TASK).extract()

    assert task_metadata.run_facets["externalQuery"].externalQueryId == "1500100900"


def test_get_table_schemas():
    # (1) Define hook mock for testing
    TASK.get_hook = mock.MagicMock()

    # (2) Mock calls to postgres
    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'ID', 1, 'int4'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'AMOUNT_OFF', 2, 'int4'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'CUSTOMER_EMAIL', 3, 'varchar'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'STARTS_ON', 4, 'timestamp'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'ENDS_ON', 5, 'timestamp')
    ]

    TASK.get_hook.return_value \
        .get_conn.return_value \
        .cursor.return_value \
        .fetchall.return_value = rows

    # (3) Extract table schemas for task
    extractor = SnowflakeCheckExtractor(TASK)
    table_schemas = extractor._get_table_schemas(table_names=[DB_TABLE_NAME])

    assert table_schemas == [DB_TABLE_SCHEMA]
