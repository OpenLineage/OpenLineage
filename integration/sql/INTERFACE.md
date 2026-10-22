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
call has an independent context.

Java callers can supply the same defaults through the four-argument overload:

```java
OpenLineageSql.parse(
    Collections.singletonList("INSERT INTO target SELECT id FROM source"),
    "postgres",
    "public",
    "mydb");
```

The existing one- and two-argument Java methods retain their behavior. Passing
`null` for a default leaves that component unspecified.

Omitting `default_database` or passing `None` preserves the existing behavior.
Rust callers can use `parse_multiple_statements_with_default_database`; the existing
`parse_multiple_statements` and `parse_sql` functions retain their signatures.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
