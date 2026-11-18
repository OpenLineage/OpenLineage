// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql, test_sql_dialect};
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

/// Each column-lineage edge as `descendant <- origin.column`.
fn edges(sql: &str, dialect: &str) -> Vec<String> {
    test_sql_dialect(sql, dialect)
        .unwrap()
        .column_lineage
        .iter()
        .flat_map(|c| {
            c.lineage.iter().map(move |l| {
                format!(
                    "{} <- {}.{}",
                    c.descendant.name,
                    l.origin
                        .as_ref()
                        .map_or(String::new(), |o| o.qualified_name()),
                    l.name
                )
            })
        })
        .collect()
}

#[test]
fn cte_reference_in_another_case_traces_columns_to_the_table() {
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT id FROM raw.orders) INSERT INTO tgt SELECT id FROM orders",
            "snowflake",
        ),
        vec!["id <- raw.orders.id"]
    );
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT id FROM raw.orders) SELECT o.id FROM orders o",
            "snowflake",
        ),
        vec!["id <- raw.orders.id"]
    );
    assert_eq!(
        edges(
            "WITH \"c\" AS (SELECT id FROM src) SELECT id FROM C",
            "postgres",
        ),
        vec!["id <- src.id"]
    );
}

#[test]
fn cte_chain_in_another_case_traces_columns_to_the_table() {
    assert_eq!(
        edges(
            "WITH Base AS (SELECT id FROM src), Renamed AS (SELECT id AS key FROM base) \
             SELECT key FROM renamed",
            "snowflake",
        ),
        vec!["key <- src.id"]
    );
}

#[test]
fn quoted_cte_name_keeps_a_differently_folded_reference_as_a_table() {
    assert_eq!(
        edges(
            "WITH \"C\" AS (SELECT id FROM src) SELECT id FROM c",
            "postgres",
        ),
        vec!["id <- c.id"]
    );
}

#[test]
fn column_qualified_by_cte_name_in_another_case_traces_to_the_table() {
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT id FROM raw.orders) SELECT orders.id FROM orders",
            "snowflake",
        ),
        vec!["id <- raw.orders.id"]
    );
}

#[test]
fn table_alias_takes_precedence_over_a_cte_of_the_same_folded_name() {
    assert_eq!(
        edges(
            "WITH c AS (SELECT x FROM s) SELECT C.x FROM t AS C",
            "snowflake",
        ),
        vec!["x <- t.x"]
    );
}
