// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::{parse_sql, BigQueryDialect, TableLineage};

#[test]
fn select_simple() {
    assert_eq!(
        test_sql("SELECT * FROM table0;",).unwrap().table_lineage,
        TableLineage {
            in_tables: tables(vec!["table0"]),
            out_tables: vec![]
        }
    )
}
#[test]
fn select_from_schema_table() {
    assert_eq!(
        test_sql("SELECT * FROM schema0.table0;",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["schema0.table0"]),
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
        )
        .unwrap()
        .table_lineage,
        TableLineage {
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
        )
        .unwrap()
        .table_lineage,
        TableLineage {
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
        )
        .unwrap()
        .table_lineage,
        TableLineage {
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
            &BigQueryDialect,
            None
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["random-project.dbt_test1.source_table"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_into() {
    assert_eq!(
        test_sql("SELECT * INTO table0 FROM table1;",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["table1"]),
            out_tables: tables(vec!["table0"])
        }
    )
}

#[test]
fn select_redshift() {
    assert_eq!(
        test_sql_dialect("SELECT [col1] FROM [test_schema].[test_table]", "redshift")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["test_schema.test_table"]),
            out_tables: vec![]
        }
    )
}
