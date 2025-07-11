# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Optional, Union

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
    sql: Union[List[str], str],
    dialect: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> Optional[SqlMeta]:
    if isinstance(sql, str):
        sql = [sql]
    try:
        return parse_sql(sql, dialect=dialect, default_schema=default_schema)
    except BaseException as e:
        # PanicException is a BaseException https://pyo3.rs/main/doc/pyo3/panic/struct.panicexception
        log.exception("SQL parser failed: %s", e)
        return None
