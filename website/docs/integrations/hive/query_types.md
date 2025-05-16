---
sidebar_position: 3
title: Query types
---

This integration supports a wide range of Hive query types, including:

* `CREATE TABLE AS SELECT` (`CTAS`): Captures lineage from source tables to the
  newly created table. Includes operations like `SELECT`, `JOIN`, `WHERE`
  filters, and aggregations within the `CTAS` statement.
* `INSERT` (`OVERWRITE TABLE` | `INTO TABLE`): Captures lineage from source data
  to the destination table. Includes operations like `SELECT`, `JOIN`, `WHERE`
  filters, and aggregations within the `INSERT` statement.
* `SELECT` statements: Do not emit lineage events on their own (as they don't
  change data). However, intermediate transformations within a `SELECT` used in
  a `CTAS` or `INSERT` are analyzed for column-level lineage.
* Complex Queries: Supports complex queries involving Common Table Expressions
  (CTEs), joins, filters, aggregations, sorting, window functions, and more.
* Union statements: `UNION ALL` statements are supported capturing lineage from
  multiple input tables to a single destination.