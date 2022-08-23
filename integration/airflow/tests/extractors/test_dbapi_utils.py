# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock

from openlineage.airflow.extractors.dbapi_utils import (
    get_table_schemas,
    create_filter_clauses,
)
from openlineage.common.dataset import Source, Dataset
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta

DB_NAME = 'FOOD_DELIVERY'
DB_SCHEMA_NAME = 'PUBLIC'
DB_TABLE_NAME = DbTableMeta('DISCOUNTS')
DB_TABLE_COLUMNS = [
    DbColumn(name="ID", type="int4", ordinal_position=1),
    DbColumn(name="AMOUNT_OFF", type="int4", ordinal_position=2),
    DbColumn(name="CUSTOMER_EMAIL", type="varchar", ordinal_position=3),
    DbColumn(name="STARTS_ON", type="timestamp", ordinal_position=4),
    DbColumn(name="ENDS_ON", type="timestamp", ordinal_position=5),
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME, table_name=DB_TABLE_NAME, columns=DB_TABLE_COLUMNS
)


def test_get_table_schemas():
    hook = MagicMock()
    # (2) Mock calls to database
    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "CUSTOMER_EMAIL", 3, "varchar"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "STARTS_ON", 4, "timestamp"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ENDS_ON", 5, "timestamp"),
    ]

    hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, rows]

    source = Source(scheme="bigquery", authority=None, connection_url=None, name=None)

    table_schemas = get_table_schemas(
        hook=hook,
        source=source,
        database=DB_NAME,
        in_query="fake_sql",
        out_query="another_fake_sql",
    )

    assert table_schemas == (
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME)],
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME)],
    )


def test_get_table_schemas_with_mixed_databases():
    hook = MagicMock()
    ANOTHER_DB_NAME = "ANOTHER_DB"

    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "CUSTOMER_EMAIL", 3, "varchar", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "STARTS_ON", 4, "timestamp", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ENDS_ON", 5, "timestamp", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4", ANOTHER_DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4", ANOTHER_DB_NAME),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "CUSTOMER_EMAIL",
            3,
            "varchar",
            ANOTHER_DB_NAME,
        ),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "STARTS_ON",
            4,
            "timestamp",
            ANOTHER_DB_NAME,
        ),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "ENDS_ON",
            5,
            "timestamp",
            ANOTHER_DB_NAME,
        ),
    ]

    hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    source = Source(scheme="bigquery", authority=None, connection_url=None, name=None)

    table_schemas = get_table_schemas(
        hook=hook,
        source=source,
        database=DB_NAME,
        in_query="fake_sql",
        out_query="another_fake_sql",
    )

    assert table_schemas == (
        [
            Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME),
            Dataset.from_table_schema(source, DB_TABLE_SCHEMA, ANOTHER_DB_NAME),
        ],
        [],
    )


def test_get_table_schemas_with_mixed_schemas():
    hook = MagicMock()
    ANOTHER_DB_SCHEMA_NAME = "ANOTHER_DB_SCHEMA"
    ANOTHER_DB_TABLE_SCHEMA = DbTableSchema(
        schema_name=ANOTHER_DB_SCHEMA_NAME,
        table_name=DB_TABLE_NAME,
        columns=DB_TABLE_COLUMNS,
    )

    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "CUSTOMER_EMAIL", 3, "varchar"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "STARTS_ON", 4, "timestamp"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ENDS_ON", 5, "timestamp"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "CUSTOMER_EMAIL", 3, "varchar"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "STARTS_ON", 4, "timestamp"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ENDS_ON", 5, "timestamp"),
    ]

    hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    source = Source(scheme="bigquery", authority=None, connection_url=None, name=None)

    table_schemas = get_table_schemas(
        hook=hook,
        source=source,
        database=DB_NAME,
        in_query="fake_sql",
        out_query="another_fake_sql",
    )

    assert table_schemas == (
        [
            Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME),
            Dataset.from_table_schema(source, ANOTHER_DB_TABLE_SCHEMA, DB_NAME),
        ],
        [],
    )


def test_get_table_schemas_with_other_database():
    hook = MagicMock()
    ANOTHER_DB_NAME = "ANOTHER_DB"

    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4", ANOTHER_DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4", ANOTHER_DB_NAME),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "CUSTOMER_EMAIL",
            3,
            "varchar",
            ANOTHER_DB_NAME,
        ),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "STARTS_ON",
            4,
            "timestamp",
            ANOTHER_DB_NAME,
        ),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "ENDS_ON",
            5,
            "timestamp",
            ANOTHER_DB_NAME,
        ),
    ]

    hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, rows]

    source = Source(scheme="bigquery", authority=None, connection_url=None, name=None)

    table_schemas = get_table_schemas(
        hook=hook,
        source=source,
        database=DB_NAME,
        in_query="fake_sql",
        out_query="another_fake_sql",
    )

    assert table_schemas == (
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, ANOTHER_DB_NAME)],
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, ANOTHER_DB_NAME)],
    )


def test_create_filter_clauses():
    assert create_filter_clauses({None: ["C1", "C2"]}) == [
        "( table_name IN ('C1','C2') )"
    ]
    assert create_filter_clauses(
        {"Schema1": ["Table1"], "Schema2": ["Table2"]}
    ) == [
        "( table_schema = 'SCHEMA1' AND table_name IN ('Table1') )",
        "( table_schema = 'SCHEMA2' AND table_name IN ('Table2') )",
    ]
    assert create_filter_clauses(
        {"Schema1": ["Table1", "Table2"]}
    ) == ["( table_schema = 'SCHEMA1' AND table_name IN ('Table1','Table2') )"]
