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

#[test]
fn table_alias_in_another_case_takes_precedence_over_a_cte() {
    assert_eq!(
        edges(
            "WITH c AS (SELECT x FROM s) SELECT c.x FROM t AS C",
            "snowflake"
        ),
        vec!["x <- t.x"]
    );
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT x FROM s) SELECT orders.x FROM t AS ORDERS",
            "snowflake"
        ),
        vec!["x <- t.x"]
    );
}

#[test]
fn qualifier_naming_an_unaliased_table_in_from_is_not_a_cte() {
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT id FROM wrong_src) SELECT orders.id FROM raw.orders",
            "snowflake"
        ),
        vec!["id <- raw.orders.id"]
    );
    let mut joined = edges(
        "WITH Orders AS (SELECT id FROM wrong_src) \
         SELECT o.id, orders.id AS id2 FROM Orders AS o JOIN raw.orders ON o.id = orders.id",
        "snowflake",
    );
    joined.sort();
    assert_eq!(joined, vec!["id <- wrong_src.id", "id2 <- raw.orders.id"]);
}

#[test]
fn qualifier_quoted_differently_from_its_cte_traces_to_the_table() {
    for (sql, dialect) in [
        (
            "WITH \"c\" AS (SELECT id FROM src) SELECT c.id FROM c",
            "postgres",
        ),
        (
            "WITH orders AS (SELECT id FROM src) SELECT `orders`.id FROM `orders`",
            "bigquery",
        ),
        (
            "WITH ORDERS AS (SELECT id FROM src) SELECT \"ORDERS\".id FROM \"ORDERS\"",
            "snowflake",
        ),
        (
            "WITH orders AS (SELECT id FROM src) SELECT [orders].id FROM [orders]",
            "mssql",
        ),
    ] {
        assert_eq!(
            edges(sql, dialect),
            vec!["id <- src.id"],
            "{dialect}: {sql}"
        );
    }
}

#[test]
fn recursive_self_reference_in_another_case_adds_no_column_lineage() {
    // Same output as the self-reference written in the CTE's own case.
    for (differing, matching, dialect) in [
        (
            "WITH RECURSIVE Nums AS (SELECT id AS n FROM seed UNION ALL SELECT n FROM nums) \
             INSERT INTO tgt SELECT n FROM nums",
            "WITH RECURSIVE Nums AS (SELECT id AS n FROM seed UNION ALL SELECT n FROM Nums) \
             INSERT INTO tgt SELECT n FROM Nums",
            "postgres",
        ),
        (
            "WITH Nums AS (SELECT id AS n FROM seed UNION ALL SELECT n FROM nums) \
             SELECT n FROM Nums",
            "WITH Nums AS (SELECT id AS n FROM seed UNION ALL SELECT n FROM Nums) \
             SELECT n FROM Nums",
            "snowflake",
        ),
    ] {
        assert_eq!(
            edges(differing, dialect),
            edges(matching, dialect),
            "{dialect}"
        );
    }
}

#[test]
fn identifier_function_in_single_table_select_resolves_the_cte() {
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT id FROM raw.orders) SELECT id FROM IDENTIFIER('orders')",
            "snowflake"
        ),
        vec!["id <- raw.orders.id"]
    );
}

#[test]
fn qualifier_binds_to_the_from_clause_under_the_dialects_folding() {
    // No CTE involved: on main these reported tables that do not exist
    // (`orders`, `ORDERS`, `o`).
    assert_eq!(
        edges("SELECT orders.id FROM raw.orders", "snowflake"),
        vec!["id <- raw.orders.id"]
    );
    assert_eq!(
        edges("SELECT ORDERS.id FROM orders", "snowflake"),
        vec!["id <- orders.id"]
    );
    assert_eq!(
        edges("SELECT o.id FROM raw.orders AS O", "snowflake"),
        vec!["id <- raw.orders.id"]
    );
}
