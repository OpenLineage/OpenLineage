# SPDX-License-Identifier: Apache-2.0.
import logging
from typing import Optional, Union, List


log = logging.getLogger(__name__)


try:
    from openlineage_sql import parse as parse_sql, SqlMeta, DbTableMeta, provider  # noqa: F401
except ImportError:
    from openlineage.common.sql.parser import parse as parse_sql, SqlMeta, DbTableMeta, provider  # noqa: F401,E501


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
