// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

//! Wildcard expansion beyond the CTE case.
//!
//! The CTE form — `WITH d AS (...) SELECT * FROM d` — is handled by the column
//! registry these tests build on. Two things are not.
//!
//! The first is the form issue #3809 actually reports: a wildcard over an
//! inline derived table, `SELECT * FROM (SELECT a, b, c FROM t)`. The columns
//! are known inside the query exactly as they are for a CTE, so the same
//! uplift applies.
//!
//! The second is that the modifiers attached to a wildcard are ignored.
//! `SELECT * EXCLUDE (secret)` reports `secret`, which is worse than reporting
//! nothing: a consumer sees an ordinary lineage edge for a column that is not
//! in the output.

use crate::test_utils::{table, test_sql, test_sql_dialect};
use openlineage_sql::{ColumnLineage, ColumnMeta};

/// `descendant <- origin.source`, in the order the parser emits them.
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
fn test_derived_table_wildcard() {
    // The example in the issue. Identical in meaning to
    // `INSERT INTO t1 SELECT a, b, c FROM t2`, which already works.
    let output = test_sql("INSERT INTO t1 SELECT * FROM (SELECT a, b, c FROM t2)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            edge("a", "t2", "a"),
            edge("b", "t2", "b"),
            edge("c", "t2", "c"),
        ]
    );
}

#[test]
fn test_derived_table_qualified_wildcard() {
    // The second form in the issue: the derived table is aliased and the
    // wildcard is qualified by that alias.
    let output = test_sql("INSERT INTO t1 SELECT tbl.* FROM (SELECT a, b, c FROM t2) tbl").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            edge("a", "t2", "a"),
            edge("b", "t2", "b"),
            edge("c", "t2", "c"),
        ]
    );
}

#[test]
fn test_derived_table_wildcard_with_extra_column() {
    // A wildcard alongside an explicit projection: both must survive.
    let output = test_sql("SELECT *, x AS x2 FROM (SELECT id, x FROM t)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            edge("id", "t", "id"),
            edge("x", "t", "x"),
            edge("x2", "t", "x"),
        ]
    );
}

#[test]
fn test_derived_table_inner_star_has_no_lineage() {
    // The boundary. `SELECT *` over a physical table has no column list to
    // uplift, and the parser has no schema, so it must stay silent rather than
    // guess. This mirrors the CTE case's own negative test.
    let output = test_sql("SELECT * FROM (SELECT * FROM physical_table)").unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_wildcard_exclude_omits_the_excluded_column() {
    // Snowflake's EXCLUDE. `secret` is not in the output, so reporting lineage
    // for it is a wrong edge rather than a missing one.
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, secret FROM t) SELECT * EXCLUDE (secret) FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![edge("id", "t", "id")]);
}

#[test]
fn test_wildcard_except_omits_the_excluded_column() {
    // The same idea spelled EXCEPT, which is what BigQuery and DuckDB use.
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, secret FROM t) SELECT * EXCEPT (secret) FROM d",
        "bigquery",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![edge("id", "t", "id")]);
}

#[test]
fn test_wildcard_rename_uses_the_new_name() {
    // RENAME changes the output column's name. The lineage should follow the
    // name a consumer will actually see.
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, x FROM t) SELECT * RENAME (x AS y) FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("id", "t", "id"), edge("y", "t", "x")]
    );
}

#[test]
fn test_cte_column_aliases_rename_the_output() {
    // `WITH d(x, y)` publishes x and y, whatever the inner query called them,
    // so the wildcard expands to the exposed names and the lineage reaches
    // back to the source columns.
    let output = test_sql("WITH d(x, y) AS (SELECT a, b FROM t) SELECT * FROM d").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("x", "t", "a"), edge("y", "t", "b")]
    );
}

#[test]
fn test_derived_table_column_aliases_rename_the_output() {
    // The same rule for the inline form.
    let output = test_sql("SELECT * FROM (SELECT a, b FROM t) AS d(x, y)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("x", "t", "a"), edge("y", "t", "b")]
    );
}

#[test]
fn test_partial_column_aliases_leave_the_rest_alone() {
    // Aliases bind positionally and may run out before the columns do: `x`
    // takes the place of `a`, and `b` keeps its own name. The output is sorted
    // by descendant, so `b` comes first here.
    let output = test_sql("WITH d(x) AS (SELECT a, b FROM t) SELECT * FROM d").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("b", "t", "b"), edge("x", "t", "a")]
    );
}

#[test]
fn test_inner_cte_does_not_leak_into_the_outer_query() {
    // Two CTEs named `d` in different scopes, with the nested one in the FROM
    // clause so that it is visited before the outer wildcard expands. The
    // outer `*` must expand to the outer `d`, which is the one in scope where
    // it is written; a registry shared across scopes answers with the inner
    // one instead.
    let output = test_sql(
        "WITH d AS (SELECT a, b FROM t) \
         SELECT d.* FROM d, (WITH d AS (SELECT c FROM u) SELECT c FROM d) AS nested",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("a", "t", "a"), edge("b", "t", "b")]
    );
}

#[test]
fn test_exclude_matches_an_unquoted_column_whatever_its_case() {
    // Snowflake resolves an unquoted identifier without regard to case, so
    // EXCLUDE (SECRET) removes `secret`. Byte equality reported an edge for a
    // column that is not in the output.
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, secret FROM t) SELECT * EXCLUDE (SECRET) FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![edge("id", "t", "id")]);
}

#[test]
fn test_rename_matches_an_unquoted_column_whatever_its_case() {
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, x FROM t) SELECT * RENAME (X AS y) FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("id", "t", "id"), edge("y", "t", "x")]
    );
}

#[test]
fn test_column_aliases_may_permute_existing_names() {
    // The aliases are the projected names in the other order, so each rename
    // targets a name the other column currently holds. Renaming one at a time
    // would overwrite the second entry before it is read.
    let output = test_sql("WITH d(y, x) AS (SELECT x, y FROM t) SELECT * FROM d").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("x", "t", "y"), edge("y", "t", "x")]
    );
}

#[test]
fn test_joined_derived_table_wildcard_expands() {
    // A derived table on the right of a join registers its columns in the
    // join's own frame, which is popped before the projection is reached.
    let output = test_sql("SELECT d.* FROM t JOIN (SELECT a, b FROM t2) d ON t.id = d.a").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("a", "t2", "a"), edge("b", "t2", "b")]
    );
}

#[test]
fn test_ilike_suppresses_expansion() {
    // ILIKE keeps the columns whose names match the pattern. Which ones those
    // are is not decided here, so reporting all of them would name columns the
    // output does not contain.
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, secret FROM t) SELECT * ILIKE '%id%' FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_replace_omits_the_replaced_column() {
    // REPLACE keeps `x` in the output but fills it from an expression, so the
    // passthrough edge would name the wrong source. The other columns expand.
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, x FROM t) SELECT * REPLACE (x * 2 AS x) FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![edge("id", "t", "id")]);
}

#[test]
fn test_column_aliases_bind_to_positions_not_to_sources() {
    // The first projection is a constant, which has no lineage but does hold a
    // position: `x` names it, and `y` is the column sourced from `t.a`.
    let output =
        test_sql("WITH d(x, y) AS (SELECT 1 AS constant, a FROM t) SELECT * FROM d").unwrap();
    assert_eq!(output.column_lineage, vec![edge("y", "t", "a")]);
}

#[test]
fn test_constant_columns_report_no_lineage() {
    // A constant has nothing upstream, so a wildcard over the table must not
    // invent an edge sourced from the table itself.
    let output = test_sql("WITH d AS (SELECT 1 AS c, a FROM t) SELECT * FROM d").unwrap();
    assert_eq!(output.column_lineage, vec![edge("a", "t", "a")]);
}

#[test]
fn test_physical_table_alias_does_not_borrow_a_cte_of_the_same_name() {
    // `d` here is an alias of `physical`, whose columns are unknown to the
    // parser. The CTE that shares the name describes something else entirely.
    let output = test_sql("WITH d AS (SELECT a FROM src) SELECT d.* FROM physical AS d").unwrap();
    assert_eq!(output.column_lineage, vec![]);
}
