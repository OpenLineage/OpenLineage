// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use openlineage_sql::{parse_sql, BigQueryDialect, SqlMeta};
use std::sync::Arc;

#[macro_use]
mod test_utils;
use test_utils::*;

#[test]
fn select_simple() {
    assert_eq!(
        test_sql("SELECT * FROM table0;",),
        SqlMeta {
            in_tables: table("table0"),
            out_tables: vec![]
        }
    )
}
#[test]
fn select_from_schema_table() {
    assert_eq!(
        test_sql("SELECT * FROM schema0.table0;",),
        SqlMeta {
            in_tables: table("schema0.table0"),
            out_tables: vec![]
        }
    )
}
#[test]
fn select_join() {
    assert_eq!(
        test_sql(
            "
                SELECT col0, col1, col2
                FROM table0
                JOIN table1
                ON t1.col0 = t2.col0",
        ),
        SqlMeta {
            in_tables: tables(vec!["table0", "table1"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_inner_join() {
    assert_eq!(
        test_sql(
            "
                SELECT col0, col1, col2
                FROM table0
                INNER JOIN table1
                ON t1.col0 = t2.col0",
        ),
        SqlMeta {
            in_tables: tables(vec!["table0", "table1"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_left_join() {
    assert_eq!(
        test_sql(
            "
            SELECT col0, col1, col2
            FROM table0
            LEFT JOIN table1
            ON t1.col0 = t2.col0",
        ),
        SqlMeta {
            in_tables: tables(vec!["table0", "table1"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_bigquery_excaping() {
    assert_eq!(
        parse_sql(
            "
            SELECT *
            FROM `random-project`.`dbt_test1`.`source_table`
            WHERE id = 1
            ",
            Arc::new(BigQueryDialect),
            None
        )
        .unwrap(),
        SqlMeta {
            in_tables: table("random-project.dbt_test1.source_table"),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_into() {
    assert_eq!(
        test_sql("SELECT * INTO table0 FROM table1;",),
        SqlMeta {
            in_tables: table("table1"),
            out_tables: table("table0")
        }
    )
}

#[test]
fn select_redshift() {
    assert_eq!(
        test_sql_dialect("SELECT [col1] FROM [test_schema].[test_table]", "redshift"),
        SqlMeta {
            in_tables: table("test_schema.test_table"),
            out_tables: vec![]
        }
    )
}
