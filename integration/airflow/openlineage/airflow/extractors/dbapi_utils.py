import logging
from contextlib import closing
from typing import Optional, TYPE_CHECKING, List, Tuple, Dict

from openlineage.common.dataset import Source, Dataset
from openlineage.common.models import DbTableSchema, DbColumn
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
                    source=source, table_schema=schema, database_name=database
                )
                for schema in schemas
            ]
            for schemas in query_schemas
        ]
    )


def parse_query_result(cursor) -> List[DbTableSchema]:
    schemas: Dict = {}
    for row in cursor.fetchall():
        table_schema_name: str = row[_TABLE_SCHEMA]
        table_name: DbTableMeta = DbTableMeta(row[_TABLE_NAME])
        table_column: DbColumn = DbColumn(
            name=row[_COLUMN_NAME],
            type=row[_UDT_NAME],
            ordinal_position=row[_ORDINAL_POSITION],
        )

        # Attempt to get table schema
        table_key: str = f"{table_schema_name}.{table_name}"
        table_schema: Optional[DbTableSchema] = schemas.get(table_key)

        if table_schema:
            # Add column to existing table schema.
            schemas[table_key].columns.append(table_column)
        else:
            # Create new table schema with column.
            schemas[table_key] = DbTableSchema(
                schema_name=table_schema_name,
                table_name=table_name,
                columns=[table_column],
            )
    return list(schemas.values())
