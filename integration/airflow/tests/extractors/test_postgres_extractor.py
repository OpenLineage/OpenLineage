# Copyright 2018-2022 contributors to the OpenLineage project
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
from openlineage.airflow.extractors.postgres_extractor import PostgresExtractor

PostgresOperator = try_import_from_string(
    "airflow.providers.postgres.operators.postgres.PostgresOperator"
)
PostgresHook = try_import_from_string("airflow.providers.postgres.hooks.postgres.PostgresHook")

CONN_ID = 'food_delivery_db'
CONN_URI = 'postgres://user:pass@localhost:5432/food_delivery'
CONN_URI_WITHOUT_USERPASS = 'postgres://localhost:5432/food_delivery'

DB_NAME = 'food_delivery'
DB_SCHEMA_NAME = 'public'
DB_TABLE_NAME = DbTableMeta('discounts')
DB_TABLE_COLUMNS = [
    DbColumn(
        name='id',
        type='int4',
        ordinal_position=1
    ),
    DbColumn(
        name='amount_off',
        type='int4',
        ordinal_position=2
    ),
    DbColumn(
        name='customer_email',
        type='varchar',
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
TASK = PostgresOperator(
    task_id=TASK_ID,
    postgres_conn_id=CONN_ID,
    sql=SQL,
    dag=DAG,
    database=DB_NAME
)


@mock.patch('openlineage.airflow.extractors.sql_extractor.get_table_schemas')  # noqa
@mock.patch('openlineage.airflow.extractors.sql_extractor.get_connection')
def test_extract(get_connection, mock_get_table_schemas):
    source = Source(
        scheme='postgres',
        authority='localhost:5432',
        connection_url=CONN_URI_WITHOUT_USERPASS
    )

    mock_get_table_schemas.return_value = (
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME)],
        [],
    )

    conn = Connection(
        conn_id=CONN_ID,
        conn_type='postgres',
        host='localhost',
        port='5432',
        schema='food_delivery'
    )

    get_connection.return_value = conn

    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=source,
            fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS]
        ).to_openlineage_dataset()]

    task_metadata = PostgresExtractor(TASK).extract()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


@mock.patch('openlineage.airflow.extractors.sql_extractor.get_table_schemas')  # noqa
@mock.patch('openlineage.airflow.extractors.sql_extractor.get_connection')
def test_extract_authority_uri(get_connection, mock_get_table_schemas):

    source = Source(
        scheme='postgres',
        authority='localhost:5432',
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
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=source,
            fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS]
        ).to_openlineage_dataset()]

    task_metadata = PostgresExtractor(TASK).extract()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


def test_get_connection_import_returns_none_if_not_exists():
    assert get_connection("does_not_exist") is None
    assert get_connection("does_exist") is None


@pytest.fixture
def create_connection():
    conn = Connection("does_exist", conn_type="postgres")
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


def test_get_connection_from_uri():
    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    assert PostgresExtractor.get_connection_uri(conn) == CONN_URI_WITHOUT_USERPASS


def test_get_normalized_postgres_connection_uri():
    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI.replace("postgres", "postgresql"))
    assert PostgresExtractor.get_connection_uri(conn) == CONN_URI_WITHOUT_USERPASS
