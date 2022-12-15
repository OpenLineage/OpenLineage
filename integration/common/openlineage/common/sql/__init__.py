# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional, Union, List

from openlineage_sql import (  # noqa: F401
    parse as parse_sql,  # noqa: F401
    SqlMeta,  # noqa: F401
    DbTableMeta,  # noqa: F401
    ColumnLineage,  # noqa: F401
    ColumnMeta,  # noqa: F401
    provider  # noqa: F401
)  # noqa: F401


log = logging.getLogger(__name__)


def parse(
    sql: Union[List[str], str],
    dialect: Optional[str] = None,
    default_schema: Optional[str] = None
) -> Optional[SqlMeta]:
    if isinstance(sql, str):
        sql = [sql]
    try:
        return parse_sql(sql, dialect=dialect, default_schema=default_schema)
    except Exception as e:
        log.error(f"SQL parser failed: {e}")
        return None
