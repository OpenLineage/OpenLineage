# SPDX-License-Identifier: Apache-2.0.
from typing import Optional, Union, List

try:
    from openlineage_sql import parse as parse_sql, SqlMeta, DbTableMeta, provider  # noqa: F401
except ImportError:
    from openlineage.common.sql.parser import parse as parse_sql, SqlMeta, DbTableMeta, provider  # noqa: F401,E501


def parse(
    sql: Union[List[str], str],
    dialect: Optional[str] = None,
    default_schema: Optional[str] = None
) -> SqlMeta:
    if isinstance(sql, str):
        sql = [sql]
    return parse_sql(sql, dialect=dialect, default_schema=default_schema)
