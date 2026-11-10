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

#[test]
fn test_alias_list_rename_collision_merges_ancestry() {
    // Renaming a to b collides with the existing output column b: the
    // renamed column's lineage is the union of both ancestry sets, not
    // whichever set a map collect happened to keep.
    let output = test_sql("SELECT * FROM (SELECT a, b FROM t) AS d(b)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "b".to_string(),
            },
            lineage: vec![
                ColumnMeta {
                    origin: Some(table("t")),
                    name: "a".to_string(),
                },
                ColumnMeta {
                    origin: Some(table("t")),
                    name: "b".to_string(),
                },
            ],
        }]
    );
}

#[test]
fn test_qualified_star_unquoted_qualifier_folds_down_to_quoted_alias_in_postgres() {
    // Postgres folds unquoted identifiers to lower case, so the unquoted
    // qualifier T names the identifier t - including the quoted alias
    // "t", whose stored form is exactly t.
    let output = test_sql_dialect(
        "SELECT T.* FROM (SELECT a, b FROM table1) AS \"t\"",
        "postgres",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("a", "table1", "a"), edge("b", "table1", "b")]
    );
}

#[test]
fn test_qualified_star_unquoted_qualifier_skips_quoted_alias_in_postgres() {
    // The quoted alias "T" is the exact identifier T; the unquoted
    // qualifier T folds to t in Postgres and cannot claim it.
    let output = test_sql_dialect(
        "SELECT T.* FROM (SELECT a, b FROM table1) AS \"T\"",
        "postgres",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_qualified_star_unquoted_qualifier_is_case_insensitive_in_postgres() {
    // Control: two unquoted identifiers fold to the same lower-case form.
    let output =
        test_sql_dialect("SELECT T.* FROM (SELECT a, b FROM table1) AS t", "postgres").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("a", "table1", "a"), edge("b", "table1", "b")]
    );
}

#[test]
fn test_qualified_star_quoted_qualifier_skips_unquoted_alias_in_snowflake() {
    // Snowflake folds the unquoted alias t to T, so the exact quoted
    // qualifier "t" does not name it.
    let output = test_sql_dialect(
        "SELECT \"t\".* FROM (SELECT a, b FROM table1) AS t",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_qualified_star_unquoted_qualifier_folds_up_to_quoted_alias_in_snowflake() {
    // Snowflake folds unquoted identifiers to upper case, so the
    // unquoted qualifier t names the identifier T - the quoted alias
    // "T", whose stored form is exactly T.
    let output = test_sql_dialect(
        "SELECT t.* FROM (SELECT a, b FROM table1) AS \"T\"",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![edge("a", "table1", "a"), edge("b", "table1", "b")]
    );
}

#[test]
fn test_qualified_star_unquoted_qualifier_skips_lowercase_quoted_alias_in_snowflake() {
    // Control: the unquoted qualifier T folds to T in Snowflake and
    // cannot claim the quoted alias "t", whose stored form is t.
    let output = test_sql_dialect(
        "SELECT T.* FROM (SELECT a, b FROM table1) AS \"t\"",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_qualified_star_unquoted_qualifier_skips_quoted_alias_of_other_case() {
    // Both derived tables carry quoted aliases, "t" and "T". The unquoted
    // qualifier T resolves to the exact identifier T under Snowflake rules,
    // so it must not also claim the columns of "t" through case folding.
    let output = test_sql_dialect(
        "SELECT T.* FROM (SELECT a FROM table1) AS \"t\" JOIN (SELECT b FROM table2) AS \"T\" ON true",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![edge("b", "table2", "b")]);
}

#[test]
fn test_qualified_star_backtick_qualifier_matches_alias_in_bigquery() {
    // GoogleSQL treats aliases within a query as case-insensitive,
    // quoted or not, so the backtick-quoted qualifier `d` names the
    // unquoted alias d even though the standard folding would store
    // that alias as D.
    let output = test_sql_dialect("SELECT `d`.* FROM (SELECT a FROM t) AS d", "bigquery").unwrap();
    assert_eq!(output.column_lineage, vec![edge("a", "t", "a")]);
}

#[test]
fn test_qualified_star_unquoted_qualifier_matches_backtick_alias_in_bigquery() {
    // The mirror case: the backtick-quoted alias `d` is still the
    // alias d, so the unquoted qualifier D names it in BigQuery.
    let output = test_sql_dialect("SELECT D.* FROM (SELECT a FROM t) AS `d`", "bigquery").unwrap();
    assert_eq!(output.column_lineage, vec![edge("a", "t", "a")]);
}

#[test]
fn test_qualified_star_unquoted_pair_is_case_insensitive_in_bigquery() {
    // Control: two unquoted identifiers already agree after folding.
    let output = test_sql_dialect("SELECT D.* FROM (SELECT a FROM t) AS d", "bigquery").unwrap();
    assert_eq!(output.column_lineage, vec![edge("a", "t", "a")]);
}

#[test]
fn test_qualified_star_quoted_qualifier_folds_to_unquoted_alias_in_redshift() {
    // Redshift folds even delimited identifiers to lower case by
    // default (enable_case_sensitive_identifier off), so the quoted
    // qualifier "D" names the unquoted alias D, whose stored form is
    // d. With that session setting on, Redshift would keep "D" exact
    // and this match would not happen; the parser is not given the
    // setting, so the default is what is modeled here.
    let output =
        test_sql_dialect("SELECT \"D\".* FROM (SELECT a FROM t) AS D", "redshift").unwrap();
    assert_eq!(output.column_lineage, vec![edge("a", "t", "a")]);
}

#[test]
fn test_qualified_star_quoted_qualifier_skips_unquoted_alias_in_postgres() {
    // Control: Postgres quoted identifiers are case-sensitive, so the
    // exact qualifier "D" does not name the unquoted alias D, whose
    // stored form is d.
    let output =
        test_sql_dialect("SELECT \"D\".* FROM (SELECT a FROM t) AS D", "postgres").unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

// Wildcards over CTEs. A CTE named in FROM offers its output columns to the
// wildcard the same way a derived table does, so the cases below mirror the
// derived-table ones above. CTEs are looked up by the name as written, so a
// qualified reference such as `other.d` stays a physical table.

#[test]
fn test_star_over_cte() {
    let output = test_sql("WITH d AS (SELECT a, b FROM table1) SELECT * FROM d").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b")]
    );
}

#[test]
fn test_qualified_star_over_cte() {
    let output = test_sql("WITH d AS (SELECT a, b FROM table1) SELECT d.* FROM d").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b")]
    );
}

#[test]
fn test_qualified_star_over_aliased_cte() {
    let output = test_sql("WITH d AS (SELECT a, b FROM table1) SELECT x.* FROM d AS x").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b")]
    );
}

#[test]
fn test_star_over_chained_ctes() {
    let output = test_sql(
        "WITH first AS (SELECT a, b FROM table1), second AS (SELECT * FROM first) \
         SELECT * FROM second",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b")]
    );
}

#[test]
fn test_star_over_joined_ctes() {
    let output = test_sql(
        "WITH d1 AS (SELECT a FROM table1), d2 AS (SELECT b FROM table2) \
         SELECT * FROM d1 JOIN d2 ON d1.a = d2.b",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table2", "b")]
    );
}

#[test]
fn test_qualified_star_picks_one_of_two_ctes() {
    let output = test_sql(
        "WITH d1 AS (SELECT a FROM table1), d2 AS (SELECT b FROM table2) \
         SELECT d2.* FROM d1 JOIN d2 ON d1.a = d2.b",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![lineage("b", "table2", "b")]);
}

#[test]
fn test_star_and_explicit_column_over_cte() {
    let output = test_sql("WITH d AS (SELECT a, b FROM table1) SELECT *, a AS a2 FROM d").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            lineage("a", "table1", "a"),
            lineage("a2", "table1", "a"),
            lineage("b", "table1", "b"),
        ]
    );
}

#[test]
fn test_cte_column_list_renames_by_position() {
    let output = test_sql("WITH d(x, y) AS (SELECT a, b FROM table1) SELECT * FROM d").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("x", "table1", "a"), lineage("y", "table1", "b")]
    );
}

#[test]
fn test_cte_column_list_names_resolve_explicitly() {
    // The renamed columns are the CTE's columns for explicit references too.
    let output = test_sql("WITH d(x, y) AS (SELECT a, b FROM table1) SELECT y FROM d").unwrap();
    assert_eq!(output.column_lineage, vec![lineage("y", "table1", "b")]);
}

#[test]
fn test_cte_column_list_counts_a_constant_position() {
    let output = test_sql("WITH d(k, x) AS (SELECT 1, a FROM table1) SELECT * FROM d").unwrap();
    assert_eq!(output.column_lineage, vec![lineage("x", "table1", "a")]);
}

#[test]
fn test_star_exclude_over_cte_snowflake() {
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, secret FROM table1) SELECT * EXCLUDE (SECRET) FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![lineage("id", "table1", "id")]);
}

#[test]
fn test_star_rename_over_cte_snowflake() {
    let output = test_sql_dialect(
        "WITH d AS (SELECT a, b FROM table1) SELECT * RENAME (a AS z) FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("b", "table1", "b"), lineage("z", "table1", "a")]
    );
}

#[test]
fn test_star_replace_over_cte_bigquery() {
    let output = test_sql_dialect(
        "WITH d AS (SELECT a, b, c FROM table1) SELECT * REPLACE (c AS b) FROM d",
        "bigquery",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            lineage("a", "table1", "a"),
            lineage("b", "table1", "c"),
            lineage("c", "table1", "c"),
        ]
    );
}

#[test]
fn test_star_ilike_over_cte_snowflake() {
    let output = test_sql_dialect(
        "WITH d AS (SELECT id, secret FROM table1) SELECT * ILIKE '%id%' FROM d",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![lineage("id", "table1", "id")]);
}

#[test]
fn test_star_over_qualified_name_is_not_the_cte() {
    // `other.d` names a physical table even though a CTE `d` is in scope,
    // so the wildcard has no columns to expand.
    let output = test_sql("WITH d AS (SELECT a FROM table1) SELECT * FROM other.d").unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_star_over_cte_of_star_over_physical_table() {
    // The CTE's own `*` is over a physical table, whose columns are not
    // known at this layer, so neither wildcard has anything to expand.
    let output = test_sql("WITH d AS (SELECT * FROM table1) SELECT * FROM d").unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_star_over_cte_does_not_reach_across_union() {
    // The CTE's columns belong to the FROM clause that names it: the other
    // side of the UNION reads a physical table and contributes nothing.
    let output =
        test_sql("WITH d AS (SELECT a FROM table1) SELECT * FROM d UNION ALL SELECT * FROM table2")
            .unwrap();
    assert_eq!(output.column_lineage, vec![lineage("a", "table1", "a")]);
}

#[test]
fn test_star_over_recursive_cte() {
    let output = test_sql_dialect(
        "WITH RECURSIVE r AS (SELECT id FROM src UNION ALL SELECT id FROM r) SELECT * FROM r",
        "postgres",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![lineage("id", "src", "id")]);
}

#[test]
fn test_star_over_cte_keeps_table_lineage() {
    let output = test_sql("WITH d AS (SELECT a FROM table1) SELECT * FROM d").unwrap();
    assert_eq!(output.table_lineage.in_tables, vec![table("table1")]);
}

#[test]
fn test_star_over_parenthesised_join_of_ctes() {
    let output = test_sql(
        "WITH d1 AS (SELECT a FROM table1), d2 AS (SELECT b FROM table2) \
         SELECT * FROM (d1 JOIN d2 ON d1.a = d2.b)",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table2", "b")]
    );
}

#[test]
fn test_update_from_cte_reports_no_cte_columns() {
    // A CTE's columns are offered only to a SELECT's FROM clause, where a
    // wildcard consumes them; an UPDATE ... FROM must not report them as
    // lineage of their own.
    let output = test_sql_dialect(
        "WITH d AS (SELECT a FROM table1) UPDATE table2 SET b = d.a FROM d WHERE table2.id = d.a",
        "postgres",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_column_list_at_cte_reference_renames_by_position() {
    let output = test_sql_dialect(
        "WITH d AS (SELECT a, b FROM table1) SELECT * FROM d AS x(p, q)",
        "postgres",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("p", "table1", "a"), lineage("q", "table1", "b")]
    );
}

#[test]
fn test_short_column_list_at_cte_reference_keeps_the_rest() {
    let output = test_sql_dialect(
        "WITH d AS (SELECT a, b FROM table1) SELECT * FROM d AS x(p)",
        "postgres",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("b", "table1", "b"), lineage("p", "table1", "a")]
    );
}

#[test]
fn test_column_list_at_reference_over_cte_column_list() {
    let output = test_sql_dialect(
        "WITH d(x, y) AS (SELECT a, b FROM table1) SELECT * FROM d AS z(p, q)",
        "postgres",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("p", "table1", "a"), lineage("q", "table1", "b")]
    );
}

#[test]
fn test_column_list_at_reference_with_unknown_order_offers_nothing() {
    // The CTE projects `*`, so which column a position names is not known
    // here; reporting the old names would name columns the query does not
    // output, so the wildcard expands nothing.
    let output = test_sql_dialect(
        "WITH d AS (SELECT * FROM (SELECT a, b FROM table1) s) SELECT * FROM d AS x(p, q)",
        "postgres",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_column_list_at_cte_reference_names_resolve_explicitly() {
    let output = test_sql_dialect(
        "WITH d AS (SELECT a, b FROM table1) SELECT p FROM d AS x(p, q)",
        "postgres",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![lineage("p", "table1", "a")]);
}

#[test]
fn test_qualified_star_over_one_side_of_cte_self_join() {
    let output = test_sql(
        "WITH d AS (SELECT a, b FROM table1) SELECT x.* FROM d AS x JOIN d AS y ON x.a = y.a",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![lineage("a", "table1", "a"), lineage("b", "table1", "b")]
    );
}

#[test]
fn test_cte_column_list_with_unknown_order_offers_nothing() {
    // The CTE's body projects `*`, so its column list cannot be applied by
    // position; the wildcard expands nothing rather than old names.
    let output =
        test_sql("WITH d(x, y) AS (SELECT * FROM (SELECT a, b FROM table1) s) SELECT * FROM d")
            .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_star_over_cte_with_union_body_merges_branch_columns() {
    // A set operation's output columns are named by its left branch; the
    // right branch's columns fold onto those names by position, they are
    // not extra output columns of their own.
    let output =
        test_sql("WITH d AS (SELECT a FROM t1 UNION ALL SELECT b FROM t2) SELECT * FROM d")
            .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "a".to_string(),
            },
            lineage: vec![
                ColumnMeta {
                    origin: Some(table("t1")),
                    name: "a".to_string(),
                },
                ColumnMeta {
                    origin: Some(table("t2")),
                    name: "b".to_string(),
                },
            ],
        }]
    );
}

#[test]
fn test_star_over_derived_table_with_union_body_merges_branch_columns() {
    let output = test_sql("SELECT * FROM (SELECT a FROM t1 UNION ALL SELECT b FROM t2)").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "a".to_string(),
            },
            lineage: vec![
                ColumnMeta {
                    origin: Some(table("t1")),
                    name: "a".to_string(),
                },
                ColumnMeta {
                    origin: Some(table("t2")),
                    name: "b".to_string(),
                },
            ],
        }]
    );
}

#[test]
fn test_star_ignores_subquery_in_join_condition() {
    // The subquery in the ON condition is not a FROM item: its output
    // columns are not columns of the query, so the wildcard must not
    // expand them.
    let output =
        test_sql("SELECT * FROM (SELECT a FROM t) d JOIN u ON d.a IN (SELECT id FROM s)").unwrap();
    assert_eq!(output.column_lineage, vec![lineage("a", "t", "a")]);
}

#[test]
fn test_star_ignores_subquery_in_unnest_argument() {
    let output = test_sql_dialect(
        "SELECT * FROM UNNEST(ARRAY(SELECT id FROM src)) AS n",
        "bigquery",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_derived_column_list_with_unknown_positions_offers_nothing() {
    // The inner query projects `*`, so the positions the column list
    // binds to cannot be known; the wildcard expands nothing rather than
    // reporting the inner names the query does not output.
    let output =
        test_sql("SELECT * FROM (SELECT * FROM (SELECT a, b FROM t) s) AS d(x, y)").unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

#[test]
fn test_qualified_star_matches_backtick_alias_in_databricks() {
    // Databricks quoted identifiers are case-insensitive, so the
    // qualifier `t` names the alias stamped from the unquoted name T.
    let output =
        test_sql_dialect("SELECT t.* FROM (SELECT a FROM x) AS `t`", "databricks").unwrap();
    assert_eq!(output.column_lineage, vec![lineage("a", "x", "a")]);
}

#[test]
fn test_star_exclude_quoted_matches_unquoted_column_in_snowflake() {
    // The frame canonicalizes the unquoted column id to ID, and the
    // quoted modifier "ID" names that stored form.
    let output = test_sql_dialect(
        "SELECT * EXCLUDE (\"ID\") FROM (SELECT id, x FROM t)",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![edge("x", "t", "x")]);
}

#[test]
fn test_star_over_quoted_cte_reference() {
    // The CTE is keyed by its name as written, without delimiters, so a
    // quoted reference finds it the way an unquoted one does.
    let output = test_sql("WITH \"d\" AS (SELECT a FROM t) SELECT * FROM \"d\"").unwrap();
    assert_eq!(output.column_lineage, vec![lineage("a", "t", "a")]);
    assert_eq!(output.table_lineage.in_tables, vec![table("t")]);
}
