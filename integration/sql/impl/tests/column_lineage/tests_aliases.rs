// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_nested_renaming() {
    let output = test_sql(
        "SELECT t2.a, t2.c as d
         FROM (SELECT t2.a, t2.b as c FROM table2 t2) t2",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table2")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "d".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table2")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_nested_renaming_with_join() {
    let output = test_sql(
        "SELECT t1.a, t2.c as d
         FROM table1 t1
         INNER JOIN (SELECT t2.a, t2.b as c FROM table2 t2) t2
         ON t1.a = t2.a",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "d".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table2")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_multiple_tables() {
    let output = test_sql(
        "SELECT t1.a, t2.b, t3.c
         FROM table1 t1, table2 t2, table3 t3",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table2")),
                    name: "b".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "c".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table3")),
                    name: "c".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_deeply_nested_alias_chain() {
    let output = test_sql(
        "SELECT d as e FROM (
            SELECT c as d FROM (
                SELECT b as c FROM (
                    SELECT a as b FROM table1 t
                )
            )
         )",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "e".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("table1")),
                name: "a".to_string()
            }]
        },]
    );
}

#[test]
fn test_circular_alias() {
    assert_eq!(
        test_sql("select * from test_orders as test_orders")
            .unwrap()
            .table_lineage
            .in_tables,
        vec![table("test_orders")],
    )
}

#[test]
fn test_long_alias_used() {
    assert_eq!(
        test_sql("with test_orders as (select * from a as a) select * from test_orders")
            .unwrap()
            .table_lineage
            .in_tables,
        vec![table("a")],
    )
}

#[ignore] // https://github.com/OpenLineage/OpenLineage/issues/2752
#[test]
fn test_long_alias_not_used() {
    assert_eq!(
        test_sql("with test_orders as (select * from a as a) select * from other")
            .unwrap()
            .table_lineage
            .in_tables,
        vec![table("other")],
    )
}

#[ignore] // https://github.com/OpenLineage/OpenLineage/issues/2752
#[test]
fn test_long_circular_alias() {
    assert_eq!(
        test_sql("with test_orders as (select * from test_orders as test_orders) select * from test_orders")
            .unwrap()
            .table_lineage.in_tables,
        vec![table("test_orders")],
    )
}
