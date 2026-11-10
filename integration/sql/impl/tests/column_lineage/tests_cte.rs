// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_simple_cte() {
    let output = test_sql(
        "WITH my_cte AS (
            SELECT a,b,c FROM t1
         )
         SELECT a,c FROM my_cte",
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
                    origin: Some(table("t1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "c".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t1")),
                    name: "c".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_cte_column_lineage_with_case_differing_reference() {
    // The reference MYCTE names the CTE defined as MyCte (Snowflake
    // folds unquoted names to upper case): its columns expand through
    // the CTE body, as they do for a same-spelling reference, instead
    // of pointing at the phantom CTE name.
    let output = crate::test_utils::test_sql_dialect(
        "WITH MyCte AS (SELECT a FROM t1) SELECT a FROM MYCTE",
        "snowflake",
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
                origin: Some(table("t1")),
                name: "a".to_string()
            }]
        }]
    );
}

#[test]
fn test_cte_column_lineage_rename_with_folded_reference() {
    // Postgres folds unquoted names to lower case: mycte names the
    // CTE defined as MyCte, and the renamed column traces back to the
    // body's column.
    let output = crate::test_utils::test_sql_dialect(
        "WITH MyCte AS (SELECT a AS x FROM t1) SELECT x FROM mycte",
        "postgres",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "x".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("t1")),
                name: "a".to_string()
            }]
        }]
    );
}
