# SPDX-License-Identifier: Apache-2.0.

try:
    from openlineage_sql import parse, SqlMeta, DbTableMeta, provider  # noqa: F401
except ImportError:
    from openlineage.common.sql.parser import parse, SqlMeta, DbTableMeta, provider  # noqa: F401
