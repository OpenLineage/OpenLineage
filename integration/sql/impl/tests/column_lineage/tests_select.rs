// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_multiple_derived_tables() {
    let output = test_sql(
        "SELECT t1.a, t2.b, t3.c FROM 
         (SELECT a, t.b FROM table1 t) t1,
         (SELECT t.b, t.c FROM table2 t) t2,
         (SELECT c, t.a FROM table3 t) t3",
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
fn test_deeply_nested_selects() {
    let output = test_sql(
        "SELECT a FROM (
            SELECT a FROM (
                SELECT a FROM (
                    SELECT a FROM t
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
                name: "a".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("t")),
                name: "a".to_string()
            }]
        },]
    );
}

#[test]
fn test_parenthesized_union_arm_keeps_both_arms_column_lineage() {
    // A parenthesized set-operation arm merges like an unparenthesized
    // one: the output column descends from both arms' columns, not
    // just the parenthesized arm's.
    let output = test_sql("SELECT a FROM t1 UNION ALL (SELECT a FROM t2)").unwrap();
    assert_eq!(output.column_lineage.len(), 1);
    let mut ancestors: Vec<String> = output.column_lineage[0]
        .lineage
        .iter()
        .map(|anc| format!("{}@{}", anc.name, anc.origin.as_ref().unwrap().name))
        .collect();
    ancestors.sort();
    assert_eq!(ancestors, vec!["a@t1".to_string(), "a@t2".to_string()]);
}
