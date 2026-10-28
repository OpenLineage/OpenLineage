# OpenLineage Integration Common

Common modules for OpenLineage integrations.

For setting up a development environment, see the [Python development setup guide](https://openlineage.io/docs/client/python/development/setup).

## SQL parser defaults

Integrations can pass their active database and schema to qualify unqualified
tables and input column origins:

```python
from openlineage.common.sql import parse

metadata = parse(
    "INSERT INTO target SELECT id FROM source",
    dialect="postgres",
    default_schema="public",
    default_database="mydb",
)
```

This requires native `openlineage_sql` bindings with default-database support.
Omitting `default_database` or passing `None` retains the existing native call,
including compatibility with bindings that do not accept that argument.
Explicit SQL qualifiers and dialect-specific `USE` statements take precedence
over the supplied defaults. Defaults are scoped to each parse call; callers must
provide their current connection context explicitly.
