# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Optional, Union

from openlineage_sql import (
    ColumnLineage,  # noqa: F401
    ColumnMeta,  # noqa: F401
    DbTableMeta,  # noqa: F401
    ExtractionError,  # noqa: F401
    SqlMeta,
    provider,  # noqa: F401
)
from openlineage_sql import parse as parse_sql

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
    except Exception as e:
        log.error(f"SQL parser failed: {e}")
        return None
