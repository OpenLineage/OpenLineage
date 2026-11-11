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

Explicit SQL qualifiers and dialect-specific `USE` statements take precedence
over the supplied defaults. Defaults are scoped to each parse call; callers must
provide their current connection context explicitly.

`default_database` is the third (database) level of a `database.schema.table`
name. For dialects with only one namespace level above the table, such as MySQL,
Hive, or Spark without a catalog, the parser reads a two-part name `x.t` as
schema `x`. For these dialects, pass the connection's database or namespace as
`default_schema`, not `default_database`:

```python
# MySQL connection to database "mydb"
parse("INSERT INTO mydb.t SELECT * FROM t", dialect="mysql", default_schema="mydb")
# input and output are both mydb.t
```

Passing `default_database="mydb"` instead would name the input `mydb.t` and the
output `mydb.mydb.t`.
