# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging

from openlineage_sql import (
    ColumnLineage,
    ColumnMeta,
    DbTableMeta,
    ExtractionError,
    SqlMeta,
    provider,
)
from openlineage_sql import parse as parse_sql

__all__ = [
    "ColumnLineage",
    "ColumnMeta",
    "DbTableMeta",
    "ExtractionError",
    "SqlMeta",
    "provider",
    "parse",
]

log = logging.getLogger(__name__)


def parse(
    sql: list[str] | str,
    dialect: str | None = None,
    default_schema: str | None = None,
    default_database: str | None = None,
) -> SqlMeta | None:
    """Extract lineage using optional defaults for unqualified table names."""
    if isinstance(sql, str):
        sql = [sql]
    try:
        if default_database is None:
            return parse_sql(sql, dialect=dialect, default_schema=default_schema)
        return parse_sql(
            sql,
            dialect=dialect,
            default_schema=default_schema,
            default_database=default_database,
        )
    except BaseException as e:
        # PanicException is a BaseException https://pyo3.rs/main/doc/pyo3/panic/struct.panicexception
        log.exception("SQL parser failed: %s", e)
        return None
