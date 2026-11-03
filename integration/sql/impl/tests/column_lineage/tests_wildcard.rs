// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql, test_sql_dialect};
use openlineage_sql::{ColumnLineage, ColumnMeta};

fn lineage(descendant: &str, origin_table: &str, origin_column: &str) -> ColumnLineage {
    ColumnLineage {
        descendant: ColumnMeta {
            origin: None,
            name: descendant.to_string(),
        },
        lineage: vec![ColumnMeta {
            origin: Some(table(origin_table)),
            name: origin_column.to_string(),
        }],
    }
}

#[test]
fn test_star_over_derived_table() {
    let output = test_sql("SELECT * FROM (SELECT a, b, c FROM table1)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            lineage("a", "table1", "a"),
            lineage("b", "table1", "b"),
            lineage("c", "table1", "c"),
        ]
    );
}

#[test]
fn test_star_over_aliased_derived_table() {
    let output = test_sql("SELECT * FROM (SELECT a, b, c FROM table1) AS t").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            lineage("a", "table1", "a"),
            lineage("b", "table1", "b"),
            lineage("c", "table1", "c"),
        ]
    );
}

#[test]
fn test_qualified_star_over_aliased_derived_table() {
    let output = test_sql("SELECT t.* FROM (SELECT a, b FROM table1) t").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b"),]
    );
}

#[test]
fn test_star_uses_derived_table_output_names() {
    // The descendant is the subquery's output name, not the source column.
    let output = test_sql("SELECT * FROM (SELECT a AS x, b FROM table1)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("b", "table1", "b"), lineage("x", "table1", "a"),]
    );
}

#[test]
fn test_star_alongside_explicit_columns() {
    let output = test_sql("SELECT a, * FROM (SELECT a, b FROM table1)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b"),]
    );
}

#[test]
fn test_star_over_joined_derived_tables() {
    let output =
        test_sql("SELECT * FROM (SELECT a, b FROM table1) JOIN (SELECT c FROM table2) ON true")
            .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            lineage("a", "table1", "a"),
            lineage("b", "table1", "b"),
            lineage("c", "table2", "c"),
        ]
    );
}

#[test]
fn test_star_over_nested_derived_tables() {
    let output = test_sql("SELECT * FROM (SELECT * FROM (SELECT a, b FROM table1))").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b"),]
    );
}

#[test]
fn test_star_over_plain_table_stays_empty() {
    // A plain table's columns are not known to the parser, so the wildcard
    // cannot be expanded.
    let output = test_sql("SELECT * FROM table1").unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_star_over_unresolvable_star_stays_empty() {
    // The inner wildcard cannot be expanded either, so there are no known
    // output columns to uplift.
    let output = test_sql("SELECT * FROM (SELECT * FROM table1)").unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_star_except() {
    let output = test_sql_dialect(
        "SELECT * EXCEPT (b) FROM (SELECT a, b, c FROM table1)",
        "bigquery",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("c", "table1", "c"),]
    );
}

#[test]
fn test_star_exclude() {
    let output = test_sql_dialect(
        "SELECT * EXCLUDE (b) FROM (SELECT a, b, c FROM table1)",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("c", "table1", "c"),]
    );
}

#[test]
fn test_star_rename() {
    let output = test_sql_dialect(
        "SELECT * RENAME (b AS beta) FROM (SELECT a, b FROM table1)",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("beta", "table1", "b"),]
    );
}

#[test]
fn test_star_replace() {
    // The replaced column's lineage comes from the replacement expression,
    // not from the wildcard expansion.
    let output = test_sql_dialect(
        "SELECT * REPLACE (upper(a) AS a) FROM (SELECT a, b FROM table1)",
        "bigquery",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b"),]
    );
}
