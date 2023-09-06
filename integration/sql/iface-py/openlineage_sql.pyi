# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from enum import Enum

"""
Definition of the public interface for openlineage_sql
"""

class DbTableMeta:
    """
    Represents a table in a database.
    """

    qualified_name: str
    database: str | None
    schema: str | None
    name: str
    # determines if namespace is already contained within a name, for example
    # external stage location for Snowflake
    provided_namespace: bool
    # determines if fields schema is provided by parser
    provided_field_schema: bool
    def __init__(self, name: str) -> None: ...

class ColumnMeta:
    """
    Represents a table in a database.
    """

    origin: DbTableMeta | None
    name: str
    def __init__(self, name: str, origin: DbTableMeta | None) -> None: ...

class ColumnLineage:
    """
    Represents column lineage.
    """

    descendant: ColumnMeta
    lineage: list[ColumnMeta]
    def __init__(self, descendant: ColumnMeta, lineage: list[ColumnMeta]) -> None: ...

class ExtractionError:
    """
    Represents an error during parsing of a SQL statement.
    """

    index: int
    message: str
    origin_statement: str

class SqlMeta:
    """
    Contains metadata about a SQL statement:
        - in & out table metadata
        - column lineage metadata
        - potential parsing errors
    """

    in_tables: list[DbTableMeta]
    out_tables: list[DbTableMeta]
    column_lineage: list[ColumnLineage]
    errors: list[ExtractionError]

def parse(
    sql: list[str], dialect: str | None = None, default_schema: str | None = None
) -> SqlMeta: ...
def provider() -> str: ...
