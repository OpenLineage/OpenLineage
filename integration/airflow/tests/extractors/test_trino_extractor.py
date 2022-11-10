# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from unittest import mock

from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow import DAG

from openlineage.airflow.utils import try_import_from_string
from openlineage.common.models import (
    DbTableSchema,
    DbColumn
)
from openlineage.common.sql import DbTableMeta
from openlineage.common.dataset import Source, Dataset, Field
from openlineage.airflow.extractors.trino_extractor import TrinoExtractor

TrinoOperator = try_import_from_string(
    "airflow.providers.trino.operators.trino.TrinoOperator"
)
TrinoHook = try_import_from_string("airflow.providers.trino.hooks.trino.TrinoHook")

CONN_ID = 'food_delivery_db'
CONN_URI = 'trino://user:pass@localhost:8080/food_delivery'
CONN_URI_WITHOUT_USERPASS = 'trino://localhost:8080/food_delivery'

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
TASK = TrinoOperator(
    task_id=TASK_ID,
    trino_conn_id=CONN_ID,
    sql=SQL,
    dag=DAG,
)


@mock.patch('openlineage.airflow.extractors.sql_extractor.get_table_schemas')  # noqa
@mock.patch('openlineage.airflow.extractors.sql_extractor.get_connection')
def test_extract(get_connection, mock_get_table_schemas):
    source = Source(
        scheme='trino',
        authority='localhost:8080',
        connection_url=CONN_URI_WITHOUT_USERPASS
    )

    mock_get_table_schemas.return_value = (
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME)],
        [],
    )

    conn = Connection(
        conn_id=CONN_ID,
        conn_type='trino',
        host='localhost',
        port='8080',
        schema='food_delivery',
        extra={'catalog': DB_NAME}
    )

    get_connection.return_value = conn

    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=source,
            fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS]
        ).to_openlineage_dataset()]

    task_metadata = TrinoExtractor(TASK).extract()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []
