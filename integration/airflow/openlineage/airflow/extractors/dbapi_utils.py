# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from contextlib import closing
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple

from openlineage.common.dataset import Dataset, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta

if TYPE_CHECKING:
    from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)

_TABLE_SCHEMA = 0
_TABLE_NAME = 1
_COLUMN_NAME = 2
_ORDINAL_POSITION = 3
# Use 'udt_name' which is the underlying type of column
# (ex: int4, timestamp, varchar, etc)
_UDT_NAME = 4
# Database is optional as 5th column
_TABLE_DATABASE = 5

TablesHierarchy = Dict[str, Dict[str, List[str]]]


def execute_query_on_hook(
    hook: "BaseHook",
    query: str
) -> Iterator[tuple]:
    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            return cursor.execute(query).fetchall()


def get_table_schemas(
    hook: "BaseHook",
    source: Source,
    database: str,
    in_query: Optional[str],
    out_query: Optional[str],
) -> Tuple[List[Dataset], ...]:
    """
    This function queries database for table schemas using provided hook.
    Responsibility to provide queries for this function is on particular extractors.
    If query for input or output table isn't provided, the query is skipped.
    """
    query_schemas = []

    # Do not query if we did not get both queries
    if not in_query and not out_query:
        return [], []

    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            for query in [in_query, out_query]:
                if query:
                    cursor.execute(query)
                    query_schemas.append(parse_query_result(cursor))
                else:
                    query_schemas.append([])
    return tuple(
        [
            [
                Dataset.from_table_schema(
                    source=source, table_schema=schema, database_name=db or database
                )
                for schema, db in schemas
            ]
            for schemas in query_schemas
        ]
    )


def parse_query_result(cursor) -> List[Tuple[DbTableSchema, str]]:
    schemas: Dict = {}
    for row in cursor.fetchall():
        table_schema_name: str = row[_TABLE_SCHEMA]
        table_name: DbTableMeta = DbTableMeta(row[_TABLE_NAME])
        table_column: DbColumn = DbColumn(
            name=row[_COLUMN_NAME],
            type=row[_UDT_NAME],
            ordinal_position=row[_ORDINAL_POSITION],
        )
        try:
            table_database = row[_TABLE_DATABASE]
        except IndexError:
            table_database = None

        # Attempt to get table schema
        table_key = ".".join(
            filter(None, [table_database, table_schema_name, table_name.name])
        )
        # table_key: str = f"{table_schema_name}.{table_name}"
        table_schema: Optional[DbTableSchema]
        table_schema, _ = schemas.get(table_key) or (None, None)

        if table_schema:
            # Add column to existing table schema.
            schemas[table_key][0].columns.append(table_column)
        else:
            # Create new table schema with column.
            schemas[table_key] = (
                DbTableSchema(
                    schema_name=table_schema_name,
                    table_name=table_name,
                    columns=[table_column],
                ),
                table_database,
            )
    return list(schemas.values())


def create_information_schema_query(
    columns: List[str],
    information_schema_table_name: str,
    tables_hierarchy: TablesHierarchy,
    uppercase_names: bool = False,
    allow_trailing_semicolon: bool = True,
) -> str:
    """
    This function creates query for getting table schemas from information schema.
    """
    sqls = []
    for db, schema_mapping in tables_hierarchy.items():
        filter_clauses = create_filter_clauses(schema_mapping, uppercase_names)
        source = information_schema_table_name
        if db:
            source = f"{db.upper() if uppercase_names else db}." f"{source}"
        sqls.append(
            (
                f"SELECT {', '.join(columns)} "
                f"FROM {source} "
                f"WHERE {' OR '.join(filter_clauses)}"
            )
        )
    sql = " UNION ALL ".join(sqls)

    # For some databases such as Trino, trailing semicolon can cause a syntax error.
    if allow_trailing_semicolon:
        sql += ";"

    return sql


def create_filter_clauses(schema_mapping, uppercase_names: bool = False) -> List[str]:
    filter_clauses = []
    for schema, tables in schema_mapping.items():
        table_names = ",".join(
            map(
                lambda name: f"'{name.upper() if uppercase_names else name}'",
                tables,
            )
        )
        if schema:
            filter_clauses.append(
                f"( table_schema = '{schema.upper() if uppercase_names else schema}' "
                f"AND table_name IN ({table_names}) )"
            )
        else:
            filter_clauses.append(f"( table_name IN ({table_names}) )")
    return filter_clauses
