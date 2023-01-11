# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

import pytest
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.utils.session import create_session

from openlineage.airflow.utils import get_connection, try_import_from_string
from openlineage.common.models import (
    DbTableSchema,
    DbColumn
)
from openlineage.common.sql import DbTableMeta
from openlineage.common.dataset import Source, Dataset, Field
from openlineage.airflow.extractors.mysql_extractor import MySqlExtractor

MySqlOperator = try_import_from_string("airflow.providers.mysql.operators.mysql.MySqlOperator")
MySqlHook = try_import_from_string("airflow.providers.mysql.hooks.mysql.MySqlHook")

CONN_ID = 'food_delivery_db'
CONN_URI = 'mysql://user:pass@localhost:3306/food_delivery'
CONN_URI_WITHOUT_USERPASS = 'mysql://localhost:3306/food_delivery'

DB_NAME = 'food_delivery'
DB_SCHEMA_NAME = None
DB_TABLE_NAME = DbTableMeta('discounts')
DB_TABLE_COLUMNS = [
    DbColumn(
        name='id',
        type='integer',
        ordinal_position=1
    ),
    DbColumn(
        name='amount_off',
        type='integer',
        ordinal_position=2
    ),
    DbColumn(
        name='customer_email',
        type='text',
        ordinal_position=3
    ),
    DbColumn(
        name='starts_on',
        type='timestamp',
        ordinal_position=4
    ),
    DbColumn(
        name='ends_on',
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

SQL = f"SELECT * FROM {DB_TABLE_NAME.name};"

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
DAG_DESCRIPTION = \
    'Email discounts to customers that have experienced order delays daily'

DAG = dag = DAG(
    DAG_ID,
    schedule_interval='@weekly',
    default_args=DAG_DEFAULT_ARGS,
    description=DAG_DESCRIPTION
)

TASK_ID = 'select'
TASK = MySqlOperator(
    task_id=TASK_ID,
    mysql_conn_id=CONN_ID,
    sql=SQL,
    dag=DAG,
    database=DB_NAME
)


@mock.patch('openlineage.airflow.extractors.sql_extractor.get_table_schemas')  # noqa
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_extract(get_connection, mock_get_table_schemas):
    source = Source(
        scheme='mysql',
        authority='localhost:3306',
        connection_url=CONN_URI_WITHOUT_USERPASS
    )

    mock_get_table_schemas.return_value = (
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME)],
        [],
    )

    conn = Connection(
        conn_id=CONN_ID,
        conn_type='mysql',
        host='localhost',
        port='3306',
        schema='food_delivery'
    )

    get_connection.return_value = conn

    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_TABLE_NAME.name}",
            source=source,
            fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS]
        ).to_openlineage_dataset()]

    task_metadata = MySqlExtractor(TASK).extract()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


@mock.patch('openlineage.airflow.extractors.sql_extractor.get_table_schemas')  # noqa
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_extract_authority_uri(get_connection, mock_get_table_schemas):
    source = Source(
        scheme='mysql',
        authority='localhost:3306',
        connection_url=CONN_URI_WITHOUT_USERPASS
    )

    mock_get_table_schemas.return_value = (
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME)],
        [],
    )

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_TABLE_NAME.name}",
            source=Source(
                scheme='mysql',
                authority='localhost:3306',
                connection_url=CONN_URI_WITHOUT_USERPASS
            ),
            fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS]
        ).to_openlineage_dataset()]

    task_metadata = MySqlExtractor(TASK).extract()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


def test_get_connection_import_returns_none_if_not_exists():
    assert get_connection("does_not_exist") is None
    assert get_connection("does_exist") is None


@pytest.fixture
def create_connection():
    conn = Connection("does_exist", conn_type="mysql")
    with create_session() as session:
        session.add(conn)
        session.commit()

    yield conn

    with create_session() as session:
        session.delete(conn)
        session.commit()


def test_get_connection_returns_one_if_exists(create_connection):
    conn = Connection("does_exist")
    assert get_connection("does_exist").conn_id == conn.conn_id


@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_information_schema_query(get_connection):
    extractor = MySqlExtractor(TASK)

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    same_schema_explicit = [
        DbTableMeta("SCHEMA.TABLE_A"),
        DbTableMeta("SCHEMA.TABLE_B"),
    ]

    same_schema_explicit_sql = (
            "SELECT table_schema, table_name, column_name, ordinal_position, column_type "
            "FROM information_schema.columns "
            "WHERE ( table_schema = 'SCHEMA' AND table_name IN ('TABLE_A','TABLE_B') );"
    )

    different_schema_implicit = [
        DbTableMeta("SCHEMA.TABLE_A"),
        DbTableMeta("ANOTHER_SCHEMA.TABLE_B"),
    ]

    different_schema_implicit_sql = (
        "SELECT table_schema, table_name, column_name, ordinal_position, column_type "
        "FROM information_schema.columns "
        "WHERE ( table_schema = 'SCHEMA' AND table_name IN ('TABLE_A') ) "
        "OR ( table_schema = 'ANOTHER_SCHEMA' AND table_name IN ('TABLE_B') );"
    )

    assert (
        extractor._information_schema_query(same_schema_explicit)
        == same_schema_explicit_sql
    )
    assert (
        extractor._information_schema_query(different_schema_implicit)
        == different_schema_implicit_sql
    )
