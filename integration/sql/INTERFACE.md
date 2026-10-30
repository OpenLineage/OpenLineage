# Cross-language interface

As goal of this project is provide single implementation for getting lineage from SQL jobs executed from different languages,
we need single interface that is very similar for every language.

The interface is `binary` - in contrast to web apis, we're not passing JSON strings, but real objects that have different 
composition depending on the target language. So, binary data we transmit is different for Java and Python.

The idea is simple. We need to pass SQL statement to the parser, and get two lists in response - one for `input` tables - ones we're reading data from
and one for `output` tables - the ones we're writing data to. This is `SqlMeta` object. For Java, it has two `List` members carrying this information.
For Python there is Python object with two lists.

The tables can be in different database schemas or even databases, so we return `DbTableMeta` object that contains up to three-level nested names.
This object carries three language-specific Strings carrying this information.

## Default database and schema

The parser accepts optional database and schema defaults for table names that do not
explicitly include those components. For example, in Python:

```python
from openlineage_sql import parse

metadata = parse(
    ["INSERT INTO target SELECT id FROM source"],
    dialect="postgres",
    default_schema="public",
    default_database="mydb",
)
```

Both input and output tables, and the input column lineage origins, use
the database `mydb` and schema `public`. An explicit database or schema in SQL takes
precedence over its default. Defaults initialize the parsing context; `USE`
statements can change that context according to the selected dialect. Each parse
call has an independent context. Omitting `default_database` or passing `None`
preserves the existing behavior.

### Which default to use

Table names have up to three levels: `database.schema.table`. `default_database`
fills the third (database) level of any name with fewer than three parts, and
`default_schema` fills the second level of any name with fewer than two parts.

Some dialects have only one namespace level above the table, for example MySQL,
Hive, and Spark without a catalog. In these dialects the parser reads a two-part
name `x.t` as schema `x` and table `t`. Pass the connection's database or
namespace as `default_schema`, not `default_database`. Otherwise the same table
gets two different names. For example, with `dialect="mysql"` and
`default_database="mydb"`, `INSERT INTO mydb.t SELECT * FROM t` gives the input
`mydb.t` (database `mydb`) but the output `mydb.mydb.t` (database and schema
`mydb`). With `default_schema="mydb"`, both resolve to `mydb.t`.

Use `default_database` only for dialects with three levels, such as Postgres,
Snowflake, BigQuery, or Databricks with Unity Catalog.

### Java

Java callers supply the same defaults through `ParseOptions`:

```java
OpenLineageSql.parse(
    Collections.singletonList("INSERT INTO target SELECT id FROM source"),
    "postgres",
    ParseOptions.builder().defaultSchema("public").defaultDatabase("mydb").build());
```

The existing one- and two-argument Java methods retain their behavior. Leaving a
`ParseOptions` field unset, or setting it to `null`, leaves that component
unspecified.

### Rust

Rust callers can use `parse_multiple_statements_with_default_database`; the existing
`parse_multiple_statements` and `parse_sql` functions retain their signatures.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
