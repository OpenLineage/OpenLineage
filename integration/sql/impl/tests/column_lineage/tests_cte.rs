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

#[test]
fn outer_from_item_does_not_capture_a_lateral_qualifier() {
    // `orders` inside the LATERAL subquery is the preceding `inner_src AS
    // orders`, so an outer `FROM raw.orders` must not change the result.
    // (Resolving an alias from inside a LATERAL subquery to its table is a
    // separate limitation: main reports `orders.id` for both queries.)
    let inner = "SELECT s.id FROM inner_src AS orders CROSS JOIN LATERAL (SELECT orders.id) AS s";
    assert_eq!(
        edges(
            &format!("SELECT ({inner}) AS id FROM raw.orders"),
            "postgres"
        ),
        edges(inner, "postgres")
    );
}

#[test]
fn alias_inside_a_parenthesized_join_shadows_an_outer_from_item() {
    assert_eq!(
        edges(
            "SELECT (SELECT orders.id FROM (inner_src AS orders CROSS JOIN helper)) AS id \
             FROM raw.orders",
            "postgres"
        ),
        vec!["id <- inner_src.id"]
    );
}

#[test]
fn qualifier_in_a_nested_query_does_not_bind_to_an_enclosing_from_item() {
    // Only a SELECT's own FROM clause binds a qualifier. Which outer item a
    // nested `orders` names depends on LATERAL, APPLY and the dialect, so
    // these keep the result they have on `main`.
    for (sql, dialect) in [
        (
            "SELECT (SELECT s.id FROM stage.orders CROSS JOIN (SELECT orders.id) AS s) AS id \
             FROM raw.orders",
            "postgres",
        ),
        (
            "SELECT (SELECT s.id FROM stage.orders CROSS APPLY (SELECT orders.id) AS s) AS id \
             FROM raw.orders",
            "mssql",
        ),
    ] {
        assert_eq!(
            edges(sql, dialect),
            vec!["id <- orders.id"],
            "{dialect}: {sql}"
        );
    }
}

#[test]
fn cte_does_not_capture_a_qualifier_an_enclosing_from_item_introduces() {
    // `orders` names `raw.orders` from the enclosing FROM clause in both,
    // so CTE `Orders` must not take it, and it traces to that table.
    for sql in [
        "WITH Orders AS (SELECT id FROM wrong_src) \
         SELECT id FROM (SELECT 1) t JOIN raw.orders ON 1 = (SELECT orders.id FROM helper)",
        "WITH Orders AS (SELECT id FROM wrong_src) \
         SELECT (SELECT orders.id FROM helper) AS id FROM raw.orders",
    ] {
        assert_eq!(
            edges(sql, "snowflake"),
            vec!["id <- raw.orders.id"],
            "{sql}"
        );
    }
}

#[test]
fn correlated_reference_to_a_cte_in_another_case_traces_to_the_table() {
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT id FROM raw.orders) \
             SELECT (SELECT orders.id FROM helper) AS id FROM orders",
            "snowflake"
        ),
        vec!["id <- raw.orders.id"]
    );
}

#[test]
fn unaliased_pivot_introduces_the_name_of_the_table_it_wraps() {
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT id FROM wrong_src) \
             SELECT orders.id FROM raw.orders PIVOT(SUM(amount) FOR month IN ('JAN'))",
            "snowflake"
        ),
        vec!["id <- raw.orders.id"]
    );
    assert_eq!(
        edges(
            "WITH Orders AS (SELECT id FROM wrong_src) \
             SELECT o.id FROM raw.orders AS o PIVOT(SUM(amount) FOR month IN ('JAN'))",
            "snowflake"
        ),
        vec!["id <- raw.orders.id"]
    );
}

#[test]
fn alias_of_a_cte_is_not_taken_by_a_cte_named_like_the_alias() {
    let sql = "WITH Orders AS (SELECT id FROM src1), O AS (SELECT id FROM wrong_src) \
               UPDATE tgt SET x = (SELECT o.id) FROM orders AS o";
    for dialect in ["postgres", "snowflake"] {
        let edges = edges(sql, dialect);
        assert!(
            edges.iter().all(|edge| !edge.contains("wrong_src")),
            "{dialect}: {edges:?}"
        );
    }
}

#[test]
fn lateral_inside_a_parenthesized_join_is_not_captured_by_an_outer_from_item() {
    let inner = "SELECT s.id \
                 FROM (inner_src AS orders CROSS JOIN LATERAL (SELECT orders.id) AS s)";
    assert_eq!(
        edges(
            &format!("SELECT ({inner}) AS id FROM raw.orders"),
            "postgres"
        ),
        edges(inner, "postgres")
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

fn edges_with_schema(sql: &str, dialect: &str, schema: Option<&str>) -> Vec<String> {
    let mut edges: Vec<String> = openlineage_sql::parse_sql(
        sql,
        openlineage_sql::get_dialect(dialect),
        schema.map(str::to_string),
    )
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
    .collect();
    edges.sort();
    edges
}

// #5179: a schema-qualified table spelled like a CTE is the table, also when
// the default schema makes its full name equal to the CTE's.
#[test]
fn schema_qualified_table_named_like_a_cte_is_the_table() {
    for dialect in ["postgres", "snowflake"] {
        for schema in [Some("public"), None] {
            assert_eq!(
                edges_with_schema(
                    "WITH c AS (SELECT a AS id FROM t) SELECT id FROM public.c",
                    dialect,
                    schema
                ),
                vec!["id <- public.c.id"],
                "{dialect}, default schema {schema:?}"
            );
        }
    }
}

// The bare reference still resolves through the CTE, with or without a
// default schema.
#[test]
fn bare_reference_to_a_cte_still_resolves_with_a_default_schema() {
    for dialect in ["postgres", "snowflake"] {
        assert_eq!(
            edges_with_schema(
                "WITH c AS (SELECT a AS id FROM t) SELECT id FROM c",
                dialect,
                Some("public")
            ),
            vec!["id <- public.t.a"],
            "{dialect}"
        );
        assert_eq!(
            edges_with_schema(
                "WITH c AS (SELECT a AS id FROM t) SELECT id FROM c",
                dialect,
                None
            ),
            vec!["id <- t.a"],
            "{dialect}"
        );
    }
}

// The other ways a query names a CTE or a table spelled like it, under a
// default schema: a qualifier naming the CTE, an alias of the CTE, a table
// aliased with the CTE's name, and a qualified table read by a qualifier.
#[test]
fn cte_and_table_of_the_same_name_stay_apart_under_a_default_schema() {
    let cases = [
        (
            "WITH c AS (SELECT a AS id FROM t) SELECT c.id FROM c",
            "id <- public.t.a",
        ),
        (
            "WITH c AS (SELECT a AS id FROM t) SELECT x.id FROM c AS x",
            "id <- public.t.a",
        ),
        (
            "WITH c AS (SELECT a AS id FROM t) SELECT c.id FROM u AS c",
            "id <- public.u.id",
        ),
        (
            "WITH c AS (SELECT a AS id FROM t) SELECT c.id FROM public.c",
            "id <- public.c.id",
        ),
    ];
    for dialect in ["postgres", "snowflake"] {
        for (sql, edge) in cases {
            assert_eq!(
                edges_with_schema(sql, dialect, Some("public")),
                vec![edge],
                "{dialect}: {sql}"
            );
        }
    }
}

// A CTE redefined under its own name and reading its first definition keeps
// that lineage under a default schema too, as `test_cte_with_cycles` checks
// without one. Under a default schema, a derived table named like the CTE
// it reads, and a CTE named like the table it reads, trace to the table's
// own columns.
#[test]
fn cte_reading_its_own_name_under_a_default_schema() {
    let cases = [
        (
            "WITH tab1 AS (SELECT s.col1, s.col2 FROM stg AS s), \
             tab1 AS (SELECT col1, col2 FROM tab1) SELECT col1, col2 FROM tab1",
            vec!["col1 <- public.stg.col1", "col2 <- public.stg.col2"],
        ),
        (
            "WITH c AS (SELECT a FROM src) SELECT c.a FROM (SELECT a FROM c) AS c",
            vec!["a <- public.src.a"],
        ),
        (
            "WITH orders AS (SELECT id, amount AS amt FROM orders) SELECT id, amt FROM orders",
            vec!["amt <- public.orders.amount", "id <- public.orders.id"],
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            edges_with_schema(sql, "postgres", Some("public")),
            expected,
            "{sql}"
        );
    }
}

// Whether a qualifier names an alias or a CTE is decided by the FROM clause
// of its own SELECT: an enclosing `u AS c` does not turn an inner `FROM c`
// into an alias, and a derived table aliased like a CTE is read through its
// own columns rather than the CTE's.
#[test]
fn alias_or_cte_is_decided_by_the_qualifiers_own_from_clause() {
    let cases = [
        (
            "WITH c AS (SELECT a AS id FROM t) \
             SELECT d.id FROM u AS c CROSS JOIN (SELECT c.id FROM c) AS d",
            ["id <- public.t.a", "id <- t.a"],
        ),
        (
            "WITH c AS (SELECT a AS id FROM t) SELECT c.id FROM (SELECT b AS id FROM u) AS c",
            ["id <- public.u.b", "id <- u.b"],
        ),
    ];
    for dialect in ["postgres", "snowflake"] {
        for (sql, [with_schema, without]) in cases {
            assert_eq!(
                edges_with_schema(sql, dialect, Some("public")),
                vec![with_schema],
                "{dialect}, default schema: {sql}"
            );
            assert_eq!(
                edges_with_schema(sql, dialect, None),
                vec![without],
                "{dialect}, no default schema: {sql}"
            );
        }
    }
}

// A qualifier in a nested query that its own FROM clause does not introduce
// names the FROM item of an enclosing SELECT with that name, before the CTE:
// what an alias renames (`FROM c AS c` is the CTE, `FROM u AS c` is `u`), the
// table an unaliased item names, and a derived table's own columns. A FROM
// clause of the nested query that does introduce the name wins over them.
#[test]
fn qualifier_in_a_nested_query_names_the_enclosing_from_item() {
    let cte = "WITH c AS (SELECT a AS id FROM t) ";
    let cases = [
        (
            "SELECT (SELECT c.id) AS r FROM c AS c",
            ["id <- public.t.a", "id <- t.a"],
        ),
        (
            "SELECT (SELECT c.id) AS r FROM c",
            ["id <- public.t.a", "id <- t.a"],
        ),
        (
            "SELECT (SELECT c.id) AS r FROM u AS c",
            ["id <- public.u.id", "id <- u.id"],
        ),
        // The alias is matched under the dialect's folding, as it is written
        // in its FROM clause, not as the qualifier spells it.
        (
            "SELECT (SELECT c.id) AS r FROM u AS C",
            ["id <- public.u.id", "id <- u.id"],
        ),
        (
            "SELECT (SELECT c.id) AS r FROM c AS C",
            ["id <- public.t.a", "id <- t.a"],
        ),
        // The nearest enclosing FROM item that introduces the name wins,
        // a derived table's alias included.
        (
            "SELECT (SELECT (SELECT c.id) FROM (SELECT b AS id FROM w) AS c) AS r FROM u AS c",
            ["id <- public.w.b", "id <- w.b"],
        ),
        (
            "SELECT (SELECT (SELECT c.id) FROM x AS c) AS r FROM u AS c",
            ["id <- public.x.id", "id <- x.id"],
        ),
        // An alias of the nested SELECT named like the relation the
        // enclosing alias renames (`u`) or like a table the CTE reads (`t`)
        // does not take the qualifier's table.
        (
            "SELECT (SELECT c.id FROM t AS u) AS r FROM u AS c",
            ["id <- public.u.id", "id <- u.id"],
        ),
        (
            "SELECT d.r FROM u AS c CROSS JOIN (SELECT c.id AS r FROM t AS u) AS d",
            ["r <- public.u.id", "r <- u.id"],
        ),
        (
            "SELECT (SELECT c.id FROM w AS s) AS r FROM s.c",
            ["id <- s.c.id", "id <- s.c.id"],
        ),
        (
            "SELECT (SELECT c.id FROM w AS t) AS r FROM c AS c",
            ["id <- public.t.a", "id <- t.a"],
        ),
        (
            "SELECT (SELECT c.id) AS r FROM s.c",
            ["id <- s.c.id", "id <- s.c.id"],
        ),
        (
            "SELECT (SELECT c.id) AS r FROM (SELECT b AS id FROM u) AS c",
            ["id <- public.u.b", "id <- u.b"],
        ),
        (
            "SELECT d.r FROM u AS c CROSS JOIN (SELECT c.id AS r FROM c) AS d",
            ["r <- public.t.a", "r <- t.a"],
        ),
    ];
    for dialect in ["postgres", "snowflake"] {
        for (query, [with_schema, without]) in cases {
            let sql = format!("{cte}{query}");
            assert_eq!(
                edges_with_schema(&sql, dialect, Some("public")),
                vec![with_schema],
                "{dialect}, default schema: {sql}"
            );
            assert_eq!(
                edges_with_schema(&sql, dialect, None),
                vec![without],
                "{dialect}, no default schema: {sql}"
            );
        }
    }
}

// A CTE that a nested query defines does not take a qualifier its FROM
// clause never reads: `c.id` names the enclosing `u AS c`.
#[test]
fn nested_cte_named_like_an_enclosing_alias_does_not_hide_it() {
    for sql in [
        "WITH c AS (SELECT a AS id FROM t) \
         SELECT (WITH c AS (SELECT q AS id FROM p) SELECT c.id FROM x) AS r FROM u AS c",
        "SELECT (WITH c AS (SELECT q AS id FROM p) SELECT c.id FROM x) AS r FROM u AS c",
    ] {
        for dialect in ["postgres", "snowflake"] {
            assert_eq!(
                edges_with_schema(sql, dialect, Some("public")),
                vec!["id <- public.u.id"],
                "{dialect}, default schema: {sql}"
            );
            assert_eq!(
                edges_with_schema(sql, dialect, None),
                vec!["id <- u.id"],
                "{dialect}, no default schema: {sql}"
            );
        }
    }
}
