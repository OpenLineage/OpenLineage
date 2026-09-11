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
