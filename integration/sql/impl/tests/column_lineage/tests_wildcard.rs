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

#[test]
fn test_replace_and_rename_compose() {
    // The renamed output carries the replacement expression's lineage.
    let output = test_sql_dialect(
        "SELECT * REPLACE (a + 1 AS a) RENAME (a AS b) FROM (SELECT a FROM t)",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![lineage("b", "t", "a")]);
}

#[test]
fn test_rename_before_replace_is_rejected_by_the_parser() {
    // sqlparser parses wildcard options in a single fixed-order pass
    // (REPLACE before RENAME), so with RENAME first the trailing REPLACE
    // is never consumed as a wildcard option and the statement fails to
    // parse: no lineage is extracted, and the failure is recorded in
    // `errors`. The modifier composition in the visitor is itself
    // order-independent; this pins the upstream limitation so that a
    // future sqlparser fix shows up here.
    let output = test_sql_dialect(
        "SELECT * RENAME (a AS b) REPLACE (a + 1 AS a) FROM (SELECT a FROM t)",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
    assert_eq!(output.errors.len(), 1);
}

fn edge(descendant: &str, origin: &str, source: &str) -> ColumnLineage {
    ColumnLineage {
        descendant: ColumnMeta {
            origin: None,
            name: descendant.to_string(),
        },
        lineage: vec![ColumnMeta {
            origin: Some(table(origin)),
            name: source.to_string(),
        }],
    }
}

#[test]
fn test_derived_column_alias_list_renames_the_output() {
    let output = test_sql("SELECT * FROM (SELECT a, b FROM t) AS d(x, y)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("x", "t", "a"), edge("y", "t", "b")]
    );
}

#[test]
fn test_alias_list_binds_by_position_past_a_constant() {
    let output = test_sql("SELECT * FROM (SELECT 1 AS k, a FROM t) AS d(x, y)").unwrap();
    assert_eq!(output.column_lineage, vec![edge("y", "t", "a")]);
}

#[test]
fn test_exclude_unquoted_is_case_insensitive_in_snowflake() {
    let output = test_sql_dialect(
        "SELECT * EXCLUDE (SECRET) FROM (SELECT id, secret FROM t)",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![edge("id", "t", "id")]);
}

#[test]
fn test_rename_unquoted_is_case_insensitive_in_snowflake() {
    let output = test_sql_dialect(
        "SELECT * RENAME (X AS y) FROM (SELECT id, x FROM t)",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("id", "t", "id"), edge("y", "t", "x")]
    );
}

#[test]
fn test_ilike_reports_no_column_outside_the_output() {
    let output = test_sql_dialect(
        "SELECT * ILIKE '%id%' FROM (SELECT id, secret FROM t)",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![edge("id", "t", "id")]);
}

#[test]
fn test_qualified_star_unquoted_qualifier_is_case_insensitive_in_snowflake() {
    // An unquoted qualifier folds, so T.* names the table aliased t.
    let output = test_sql_dialect(
        "SELECT T.* FROM (SELECT a, b FROM table1) AS t",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("a", "table1", "a"), edge("b", "table1", "b")]
    );
}

#[test]
fn test_qualified_star_quoted_qualifier_matches_exactly_in_snowflake() {
    let output = test_sql_dialect(
        "SELECT \"T\".* FROM (SELECT a, b FROM table1) AS \"T\"",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("a", "table1", "a"), edge("b", "table1", "b")]
    );
}

#[test]
fn test_qualified_star_quoted_qualifier_does_not_fold_in_snowflake() {
    // A quoted qualifier must match exactly: "t" does not name the table
    // aliased "T".
    let output = test_sql_dialect(
        "SELECT \"t\".* FROM (SELECT a, b FROM table1) AS \"T\"",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}
