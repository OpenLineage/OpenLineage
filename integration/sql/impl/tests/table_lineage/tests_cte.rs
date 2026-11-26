// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::{get_dialect, parse_sql, ExtractionError, TableLineage};
use sqlparser::dialect::PostgreSqlDialect;

#[test]
fn parse_simple_cte() {
    assert_eq!(
        test_sql(
            "
                WITH sum_trans as (
                    SELECT user_id, COUNT(*) as cnt, SUM(amount) as balance
                    FROM transactions
                    WHERE created_date > '2020-01-01'
                    GROUP BY user_id
                )
                INSERT INTO potential_fraud (user_id, cnt, balance)
                SELECT user_id, cnt, balance
                    FROM sum_trans
                    WHERE count > 1000 OR balance > 100000;
                ",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["transactions"]),
            out_tables: tables(vec!["potential_fraud"])
        }
    );
}

#[test]
fn parse_bugged_cte() {
    let sql = "
        WITH sum_trans (
            SELECT user_id, COUNT(*) as cnt, SUM(amount) as balance
            FROM transactions
            WHERE created_date > '2020-01-01'
            GROUP BY user_id
        )
        INSERT INTO potential_fraud (user_id, cnt, balance)
        SELECT user_id, cnt, balance
        FROM sum_trans
        WHERE count > 1000 OR balance > 100000;";
    let meta = parse_sql(sql, &PostgreSqlDialect {}, None).unwrap();
    assert_eq!(meta.errors.len(), 1);
    assert_eq!(
        meta.errors.first().unwrap(),
        &ExtractionError {
            index: 0,
            message: "Expected: ), found: ( at Line: 3, Column: 34".to_string(),
            origin_statement: sql.to_string(),
        }
    );
}

#[test]
fn parse_recursive_cte() {
    assert_eq!(
        test_sql(
            "
            WITH RECURSIVE subordinates AS
            (SELECT employee_id,
                manager_id,
                full_name
            FROM employees
            WHERE employee_id = 2
            UNION SELECT e.employee_id,
                e.manager_id,
                e.full_name
            FROM employees e
            INNER JOIN subordinates s ON s.employee_id = e.manager_id)
            INSERT INTO sub_employees (employee_id, manager_id, full_name)
            SELECT employee_id, manager_id, full_name FROM subordinates;
        "
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["employees"]),
            out_tables: tables(vec!["sub_employees"])
        }
    )
}

#[test]
fn multiple_ctes() {
    assert_eq!(
        test_sql(
            "
            WITH customers AS (
                SELECT * FROM DEMO_DB.public.stg_customers
            ),
            orders AS (
                SELECT * FROM DEMO_DB.public.stg_orders
            )
            SELECT *
            FROM customers c
            JOIN orders o
            ON c.id = o.customer_id
        "
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec![
                "DEMO_DB.public.stg_customers",
                "DEMO_DB.public.stg_orders"
            ]),
            out_tables: vec![]
        }
    )
}

#[test]
fn cte_insert_overwrite() {
    assert_eq!(
        test_sql_dialect(
            "
            WITH g_s AS (
              SELECT
                p_i,
                u_i,
                uk,
                started,
                COALESCE(stopped, updated) AS stopped,
                CASE WHEN started > MAX(
                  FROM_UNIXTIME(
                    UNIX_TIMESTAMP(
                      COALESCE(stopped, updated)
                    ) + 30
                  )
                ) OVER(
                  PARTITION BY p_i,
                  u_i,
                  uk
                  ORDER BY
                    started,
                    COALESCE(stopped, updated) ROWS BETWEEN UNBOUNDED PRECEDING
                    AND 1 PRECEDING
                ) THEN 1 ELSE 0 END gr_st
              FROM
                d_n.f_p_s
              WHERE
                ds = 'ds'
            ),
            grps AS (
              SELECT
                p_i,
                u_i,
                uk,
                started,
                stopped,
                SUM(gr_st) OVER(
                  PARTITION BY p_i,
                  u_i,
                  uk
                  ORDER BY
                    started,
                    stopped
                ) AS grp
              FROM
                g_s
            ) INSERT OVERWRITE TABLE dev_d_n.f_p_s_m PARTITION (ds = 'ds')
            SELECT
              grps.p_i,
              grps.u_i,
              grps.uk,
              MIN(grps.started) AS started,
              MIN(tps.joined) AS joined,
              MAX(grps.stopped) AS stopped,
              COLLECT_LIST(tps.id) AS s_i_l,
              COLLECT_LIST(tps.p_i_i) AS p_i_i_l
            FROM
              grps
              INNER JOIN d_n.f_p_s fps ON grps.p_i = tps.p_i
              AND grps.u_i = tps.u_i
              AND grps.uk = tps.uk
              AND tps.ds = 'ds'
              AND grps.started = tps.started
            GROUP BY
              grps.p_i,
              grps.u_i,
              grps.uk,
              grps.grp
        ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["d_n.f_p_s"]),
            out_tables: tables(vec!["dev_d_n.f_p_s_m"])
        }
    )
}

fn cte_lineage(sql: &str, dialect: &str) -> TableLineage {
    test_sql_dialect(sql, dialect).unwrap().table_lineage
}

#[test]
fn cte_reference_in_another_case_resolves_in_snowflake() {
    assert_eq!(
        cte_lineage(
            "WITH Orders AS (SELECT id FROM raw.orders) INSERT INTO tgt SELECT id FROM orders",
            "snowflake",
        ),
        TableLineage {
            in_tables: tables(vec!["raw.orders"]),
            out_tables: tables(vec!["tgt"])
        }
    );
    assert_eq!(
        cte_lineage(
            "WITH C AS (SELECT id FROM src) INSERT INTO tgt SELECT id FROM c",
            "snowflake",
        ),
        TableLineage {
            in_tables: tables(vec!["src"]),
            out_tables: tables(vec!["tgt"])
        }
    );
}

#[test]
fn cte_reference_in_another_case_resolves_in_postgres() {
    assert_eq!(
        cte_lineage(
            "WITH C AS (SELECT id FROM src) INSERT INTO tgt SELECT id FROM c",
            "postgres",
        ),
        TableLineage {
            in_tables: tables(vec!["src"]),
            out_tables: tables(vec!["tgt"])
        }
    );
    // Unquoted `C` folds to `c`, which is the quoted name exactly.
    assert_eq!(
        cte_lineage(
            "WITH \"c\" AS (SELECT id FROM src) INSERT INTO tgt SELECT id FROM C",
            "postgres",
        ),
        TableLineage {
            in_tables: tables(vec!["src"]),
            out_tables: tables(vec!["tgt"])
        }
    );
}

#[test]
fn quoted_cte_name_does_not_match_a_differently_folded_reference() {
    // Quoted "C" keeps its case, and unquoted `c` folds to `c`, so in
    // Postgres the reference names a real table rather than the CTE.
    assert_eq!(
        cte_lineage(
            "WITH \"C\" AS (SELECT id FROM src) INSERT INTO tgt SELECT id FROM c",
            "postgres",
        ),
        TableLineage {
            in_tables: tables(vec!["c"]),
            out_tables: tables(vec!["tgt"])
        }
    );
}

#[test]
fn recursive_cte_referring_to_itself_in_another_case_is_not_an_input() {
    assert_eq!(
        cte_lineage(
            "WITH RECURSIVE Nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 5) \
             INSERT INTO tgt SELECT n FROM nums",
            "postgres",
        ),
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["tgt"])
        }
    );
}

#[test]
fn bigquery_matches_cte_names_without_regard_to_case_even_when_quoted() {
    assert_eq!(
        cte_lineage(
            "INSERT INTO tgt WITH Orders AS (SELECT id FROM src) SELECT id FROM `orders`",
            "bigquery",
        ),
        TableLineage {
            in_tables: tables(vec!["src"]),
            out_tables: tables(vec!["tgt"])
        }
    );
}

fn in_tables(sql: &str, dialect: &str) -> Vec<String> {
    let mut names: Vec<String> = cte_lineage(sql, dialect)
        .in_tables
        .iter()
        .map(|t| t.qualified_name())
        .collect();
    names.sort();
    names
}

#[test]
fn reference_with_the_spelling_of_a_differently_folded_cte_is_a_table() {
    // `"c"` stays c and unquoted `c` folds to C in Snowflake; Postgres is the
    // mirror image. Neither reference names the CTE, so it is a real table.
    assert_eq!(
        in_tables(
            "WITH \"c\" AS (SELECT id FROM src) INSERT INTO tgt SELECT id FROM c",
            "snowflake"
        ),
        vec!["c"]
    );
    assert_eq!(
        in_tables(
            "WITH \"C\" AS (SELECT id FROM src) INSERT INTO tgt SELECT id FROM C",
            "postgres"
        ),
        vec!["C"]
    );
}

#[test]
fn ctes_with_one_spelling_and_different_quoting_stay_distinct() {
    assert_eq!(
        in_tables(
            "WITH \"C\" AS (SELECT id FROM src_upper), C AS (SELECT id FROM src_lower) \
             INSERT INTO tgt SELECT id FROM \"C\"",
            "postgres"
        ),
        vec!["src_upper"]
    );
    assert_eq!(
        in_tables(
            "WITH C AS (SELECT id FROM src_lower), \"C\" AS (SELECT id FROM src_upper) \
             INSERT INTO tgt SELECT id FROM c",
            "postgres"
        ),
        vec!["src_lower"]
    );
    assert_eq!(
        in_tables(
            "WITH \"orders\" AS (SELECT id FROM t1), orders AS (SELECT id FROM t2) \
             INSERT INTO tgt SELECT id FROM \"orders\"",
            "snowflake"
        ),
        vec!["t1"]
    );
}

#[test]
fn reference_resolves_to_the_cte_it_names_across_nested_scopes() {
    // The inner unquoted C folds to c; "C" names only the outer CTE.
    assert_eq!(
        in_tables(
            "WITH \"C\" AS (SELECT id FROM outer_src) INSERT INTO tgt \
             SELECT id FROM (WITH C AS (SELECT id FROM inner_src) SELECT id FROM \"C\") sub",
            "postgres"
        ),
        vec!["outer_src"]
    );
}

#[test]
fn sqlite_matches_cte_names_without_regard_to_case_even_when_quoted() {
    assert_eq!(
        in_tables(
            "WITH \"orders\" AS (SELECT id FROM src) INSERT INTO tgt SELECT id FROM orders",
            "sqlite"
        ),
        vec!["src"]
    );
    assert_eq!(
        in_tables(
            "WITH Orders AS (SELECT id FROM src) INSERT INTO tgt SELECT id FROM \"orders\"",
            "sqlite"
        ),
        vec!["src"]
    );
}

#[test]
fn ctes_folding_to_one_name_resolve_the_same_way_every_time() {
    // Snowflake rejects two CTEs that fold to one name; the parser should at
    // least answer deterministically, with the later definition.
    for _ in 0..20 {
        assert_eq!(
            in_tables(
                "WITH tab1 AS (SELECT id FROM s1), TAB1 AS (SELECT id FROM s2) \
                 INSERT INTO tgt SELECT id FROM Tab1",
                "snowflake"
            ),
            vec!["s2"]
        );
    }
}

fn in_tables_with_default_schema(sql: &str, dialect: &str, schema: &str) -> Vec<String> {
    let mut names: Vec<String> = parse_sql(sql, get_dialect(dialect), Some(schema.to_string()))
        .unwrap()
        .table_lineage
        .in_tables
        .iter()
        .map(|t| t.qualified_name())
        .collect();
    names.sort();
    names
}

#[test]
fn cte_reference_and_a_table_in_the_default_schema_stay_distinct() {
    // A CTE reference keeps its spelling as a dependency, so under a default
    // schema `ORDERS` does not become the same table as `public.orders`.
    assert_eq!(
        in_tables_with_default_schema(
            "WITH RECURSIVE orders AS \
             (SELECT * FROM public.orders UNION ALL SELECT * FROM ORDERS) \
             INSERT INTO target SELECT * FROM orders",
            "postgres",
            "public"
        ),
        vec!["public.orders"]
    );
    assert_eq!(
        in_tables_with_default_schema(
            "WITH Orders AS (SELECT id FROM src), \
             j AS (SELECT o.id FROM orders AS o JOIN raw.Orders AS r ON o.id = r.id) \
             INSERT INTO tgt SELECT id FROM j",
            "postgres",
            "raw"
        ),
        vec!["raw.Orders", "raw.src"]
    );
}

#[test]
fn cte_nested_under_the_same_folded_name_keeps_its_tables() {
    assert_eq!(
        in_tables(
            "WITH x AS (SELECT 1 AS id), \
             C AS (WITH c AS (SELECT id FROM src) SELECT id FROM c) \
             INSERT INTO tgt SELECT id FROM C",
            "snowflake"
        ),
        vec!["src"]
    );
}

#[test]
fn first_cte_with_its_own_with_keeps_the_tables_of_its_nested_ctes() {
    for (sql, dialect) in [
        (
            "WITH c AS (WITH d AS (SELECT id FROM src) SELECT id FROM d) SELECT id FROM c",
            "postgres",
        ),
        (
            "WITH Outr AS (WITH Innr AS (SELECT id FROM src) SELECT id FROM innr) \
             INSERT INTO tgt SELECT id FROM outr",
            "snowflake",
        ),
        (
            "WITH C AS (WITH c AS (SELECT id FROM src) SELECT id FROM c) \
             INSERT INTO tgt SELECT id FROM c",
            "snowflake",
        ),
    ] {
        assert_eq!(in_tables(sql, dialect), vec!["src"], "{dialect}: {sql}");
    }
}

#[test]
fn parse_cte_select_predicate_subquery_table_is_input() {
    // The statement body reads the CTE while a predicate subquery reads
    // another table: both the CTE's own input and the predicate table
    // are inputs of the statement. This is the plain-statement
    // counterpart of the multi-table insert predicate tests - the
    // collect_inputs change that stopped dropping a frame's own
    // dependencies when the frame has CTEs applies to every statement,
    // not only to multi-table inserts.
    assert_eq!(
        test_sql(
            "WITH c AS (SELECT * FROM a) \
             SELECT * FROM c WHERE id IN (SELECT id FROM b)"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "b"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn nested_with_cte_inputs_are_collected() {
    // A CTE defined in a nested WITH subquery contributes its own inputs,
    // and its name must not surface as an input itself: b reads t2, so t2
    // is the only input.
    assert_eq!(
        test_sql(
            "WITH o AS (SELECT 1) SELECT * FROM \
             (WITH b AS (SELECT * FROM t2) \
             SELECT * FROM (WITH c AS (SELECT 1 AS x) SELECT * FROM c) q \
             JOIN b ON 1=1) z",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["t2"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn nested_with_in_projection_subquery_inputs_are_collected() {
    // The same collection through a scalar subquery in the projection:
    // the nested CTE c reads a, so a and the outer CTE's input x are the
    // inputs; c itself is not.
    assert_eq!(
        test_sql_dialect(
            "WITH o AS (SELECT * FROM x) \
             SELECT (WITH c AS (SELECT * FROM a) \
             SELECT max(id) FROM (WITH d AS (SELECT 1 id) SELECT * FROM d) q \
             JOIN c ON 1=1) AS m FROM o",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "x"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn predicate_subquery_with_nested_with_inputs_are_collected() {
    // A table joined inside a predicate subquery whose FROM is a derived
    // table with its own WITH is a statement input alongside the CTE's.
    assert_eq!(
        test_sql_dialect(
            "WITH c AS (SELECT * FROM a) SELECT * FROM c \
             WHERE id IN (SELECT id FROM \
             (WITH d AS (SELECT 1 AS id) SELECT * FROM d) q JOIN b ON 1=1)",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "b"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn join_back_to_cte_adds_no_cte_input() {
    // Joining back to the statement's CTE after a nested WITH subquery
    // adds nothing: c resolves to the CTE, whose input a is already
    // counted.
    assert_eq!(
        test_sql_dialect(
            "WITH c AS (SELECT * FROM a) SELECT * FROM c \
             JOIN (WITH d AS (SELECT 1 AS n) SELECT * FROM d) q ON 1=1 \
             JOIN c c2 ON 1=1",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn nested_with_reading_outer_cte_in_predicate_subquery() {
    // The nested WITH's CTE reads the outer CTE: the reference resolves
    // through the outer CTE, whose own input is collected - the outer
    // CTE's name is not itself an input.
    assert_eq!(
        test_sql(
            "WITH c AS (SELECT * FROM a) \
             SELECT * FROM c WHERE id IN (WITH e AS (SELECT id FROM c) SELECT id FROM e)"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn nested_with_reading_outer_cte_in_derived_table() {
    // Same through a derived table: the only real table read anywhere
    // is a, reached through both CTEs.
    assert_eq!(
        test_sql(
            "WITH c AS (SELECT * FROM a) \
             SELECT * FROM (WITH e AS (SELECT * FROM c) SELECT * FROM e) z"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn cte_in_derived_table_does_not_capture_join_table() {
    // The CTE t exists only inside the derived table. The t joined in
    // the outer query is a real table and stays an input, next to the
    // CTE's own input a.
    assert_eq!(
        test_sql(
            "SELECT * FROM (WITH t AS (SELECT * FROM a) SELECT * FROM t) x \
             JOIN t ON x.id = t.id"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "t"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn cte_in_derived_table_does_not_capture_predicate_table() {
    // Same scoping through an INSERT source: the t read by the WHERE
    // predicate subquery is a real table, not the derived table's CTE.
    assert_eq!(
        test_sql(
            "INSERT INTO o \
             SELECT * FROM (WITH t AS (SELECT * FROM a) SELECT * FROM t) x \
             WHERE id IN (SELECT id FROM t)"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "t"]),
            out_tables: tables(vec!["o"])
        }
    );
}
#[test]
fn inner_cte_does_not_shadow_outer_cte() {
    // The inner WITH redefines c over b; the outer query's own c still
    // resolves to its definition over a, and both real tables are
    // inputs.
    assert_eq!(
        test_sql(
            "WITH c AS (SELECT * FROM a) \
             SELECT * FROM (SELECT * FROM (WITH c AS (SELECT * FROM b) SELECT * FROM c) y) x \
             JOIN c ON 1=1"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "b"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn cte_defined_in_cte_body_does_not_capture_join_table() {
    // d exists only inside c's body. The d joined in the outer query
    // is a real table, and c contributes its body's input a.
    assert_eq!(
        test_sql(
            "WITH c AS (WITH d AS (SELECT * FROM a) SELECT * FROM d) \
             SELECT * FROM c JOIN d ON 1=1"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "d"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn cte_does_not_leak_into_later_statement_after_use_warehouse() {
    // A Snowflake script starting with USE WAREHOUSE: the CTE defined
    // in the second statement must not capture the real table c read
    // by the third.
    assert_eq!(
        test_multiple_sql_dialect(
            vec![
                "USE WAREHOUSE wh;",
                "WITH c AS (SELECT * FROM a) SELECT * FROM c;",
                "INSERT INTO out1 SELECT * FROM c;",
            ],
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "c"]),
            out_tables: tables(vec!["out1"])
        }
    );
}
#[test]
fn cte_does_not_leak_into_later_statement_without_use() {
    // The same script without the leading USE.
    assert_eq!(
        test_multiple_sql_dialect(
            vec![
                "WITH c AS (SELECT * FROM a) SELECT * FROM c;",
                "INSERT INTO out1 SELECT * FROM c;",
            ],
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "c"]),
            out_tables: tables(vec!["out1"])
        }
    );
}
#[test]
fn cte_does_not_leak_into_later_statement_after_use_db_schema() {
    // With USE db.sch the same holds, and the defaults apply to the
    // tables of both later statements.
    assert_eq!(
        test_multiple_sql_dialect(
            vec![
                "USE db.sch;",
                "WITH c AS (SELECT * FROM a) SELECT * FROM c;",
                "INSERT INTO out1 SELECT * FROM c;",
            ],
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["db.sch.a", "db.sch.c"]),
            out_tables: tables(vec!["db.sch.out1"])
        }
    );
}
#[test]
fn qualified_join_matching_cte_name_is_an_input() {
    // Only a bare reference can name a CTE: sch.c is a real table even
    // though the statement defines a CTE called c.
    assert_eq!(
        test_sql_dialect(
            "WITH c AS (SELECT * FROM a) SELECT * FROM \
             (WITH d AS (SELECT 1 AS n) SELECT * FROM d) q JOIN sch.c ON 1=1",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["sch.c"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn subquery_cte_alias_does_not_hide_real_table_in_second_exists() {
    assert_eq!(
        test_sql(
            "SELECT * FROM a WHERE EXISTS (WITH t AS (SELECT * FROM b) SELECT * FROM t) \
             AND EXISTS (SELECT * FROM t)"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "b", "t"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn subquery_cte_alias_does_not_hide_joined_real_table() {
    assert_eq!(
        test_sql(
            "SELECT * FROM s JOIN u ON EXISTS (WITH t AS (SELECT * FROM a) SELECT * FROM t) \
             JOIN t ON s.id = t.id"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "s", "t", "u"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn subquery_cte_alias_does_not_hide_select_into_target() {
    assert_eq!(
        test_sql(
            "SELECT * INTO t FROM a WHERE EXISTS (WITH t AS (SELECT * FROM b) SELECT * FROM t)"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "b"]),
            out_tables: tables(vec!["t"])
        }
    );
}
#[test]
fn unused_outer_cte_not_marked_used_through_nested_cte_body() {
    assert_eq!(
        test_sql(
            "WITH c1 AS (SELECT * FROM a), \
             c2 AS (WITH d AS (SELECT * FROM c1) SELECT * FROM d) \
             SELECT * FROM b"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["b"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn recursive_cte_self_reference_through_nested_with_is_not_an_input() {
    assert_eq!(
        test_sql(
            "WITH RECURSIVE c AS (SELECT * FROM a UNION ALL \
             SELECT * FROM (WITH d AS (SELECT * FROM c) SELECT * FROM d) sub) \
             SELECT * FROM c"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn inner_with_does_not_promote_cte_body_to_main() {
    assert_eq!(
        test_sql(
            "WITH unused AS (WITH d AS (SELECT * FROM a) SELECT * FROM d JOIN b ON 1=1), \
             used AS (SELECT * FROM c) \
             SELECT * FROM used"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["c"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn parenthesized_with_branch_does_not_capture_later_branch_table() {
    assert_eq!(
        test_sql("(WITH t AS (SELECT * FROM a) SELECT * FROM t) UNION ALL SELECT * FROM t")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "t"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn parenthesized_with_branch_shadowing_outer_cte_keeps_both_inputs() {
    assert_eq!(
        test_sql(
            "WITH c AS (SELECT * FROM a) \
             (WITH c AS (SELECT * FROM b) SELECT * FROM c) UNION ALL SELECT * FROM c"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "b"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn used_outer_cte_through_nested_cte_body_collects_inputs() {
    assert_eq!(
        test_sql(
            "WITH c1 AS (SELECT * FROM a), \
             c2 AS (WITH d AS (SELECT * FROM c1) SELECT * FROM d) \
             SELECT * FROM c2"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn quoted_cte_referenced_with_quotes_keeps_inputs() {
    // A CTE defined and referenced with quoted names: the reference is
    // looked up by the name's value, not its display form, so the CTE
    // resolves and its input is collected.
    for dialect in ["snowflake", "postgres"] {
        assert_eq!(
            test_sql_dialect(
                "WITH \"c\" AS (SELECT * FROM a) SELECT * FROM \"c\"",
                dialect,
            )
            .unwrap()
            .table_lineage,
            TableLineage {
                in_tables: tables(vec!["a"]),
                out_tables: tables(vec![])
            }
        );
    }
}
#[test]
fn quoted_cte_referenced_from_insert_source_and_join() {
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO t WITH \"c\" AS (SELECT * FROM a) SELECT * FROM \"c\"",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec!["t"])
        }
    );
    assert_eq!(
        test_sql_dialect(
            "WITH \"c\" AS (SELECT * FROM a) SELECT * FROM \"c\" JOIN b ON 1=1",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "b"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn cte_referenced_with_different_unquoted_case_keeps_inputs() {
    // Unquoted identifiers fold case (upper in Snowflake, lower in
    // Postgres), so MYCTE names the CTE defined as MyCte: it resolves,
    // its input is collected, and no phantom MYCTE table appears.
    for dialect in ["snowflake", "postgres"] {
        assert_eq!(
            test_sql_dialect(
                "WITH MyCte AS (SELECT * FROM a) SELECT * FROM MYCTE",
                dialect,
            )
            .unwrap()
            .table_lineage,
            TableLineage {
                in_tables: tables(vec!["a"]),
                out_tables: tables(vec![])
            }
        );
    }
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO t WITH MyCte AS (SELECT * FROM a) SELECT * FROM MYCTE",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec!["t"])
        }
    );
    // The same resolution through a derived table's body.
    assert_eq!(
        test_sql_dialect(
            "WITH MyCte AS (SELECT * FROM a) SELECT * FROM (SELECT * FROM MYCTE) d",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn quoted_cte_with_different_quoted_case_is_a_real_table() {
    // Quoted identifiers are case-sensitive: "c" does not name the CTE
    // "C", so it is a real table input and the unused CTE contributes
    // nothing.
    let mut quoted_c = table("c");
    quoted_c.quote_style = Some(openlineage_sql::QuoteStyle {
        database: None,
        schema: None,
        name: Some('"'),
    });
    assert_eq!(
        test_sql_dialect(
            "WITH \"C\" AS (SELECT * FROM a) SELECT * FROM \"c\"",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![quoted_c],
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn quoted_and_unquoted_ctes_with_folding_names_coexist() {
    // In Snowflake, "mycte" (quoted, stored as written) and MyCte
    // (unquoted, stored folded to MYCTE) are two different CTEs. Their
    // registration keys are their stored forms, so each reference
    // resolves to its own CTE.
    let with = "WITH \"mycte\" AS (SELECT * FROM quoted_src), \
                MyCte AS (SELECT * FROM unquoted_src) ";
    assert_eq!(
        test_sql_dialect(&format!("{with} SELECT * FROM \"mycte\""), "snowflake")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["quoted_src"]),
            out_tables: tables(vec![])
        }
    );
    assert_eq!(
        test_sql_dialect(&format!("{with} SELECT * FROM MyCte"), "snowflake")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["unquoted_src"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn cte_stored_form_keying_through_set_op_and_predicate() {
    // The same stored-form resolution through a set-operation arm and
    // through a predicate subquery.
    let with = "WITH \"mycte\" AS (SELECT * FROM quoted_src), \
                MyCte AS (SELECT * FROM unquoted_src) ";
    assert_eq!(
        test_sql_dialect(
            &format!("{with} SELECT * FROM \"mycte\" UNION ALL SELECT * FROM \"mycte\""),
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["quoted_src"]),
            out_tables: tables(vec![])
        }
    );
    assert_eq!(
        test_sql_dialect(
            &format!("{with} SELECT * FROM base WHERE EXISTS (SELECT 1 FROM \"mycte\")"),
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["base", "quoted_src"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn sibling_cte_reading_quoted_table_matching_another_ctes_fold() {
    // CTE `other` reads the real quoted table "mycte"; the sibling CTE
    // MyCte folds to MYCTE and must not capture that reference.
    let mut quoted_mycte = table("mycte");
    quoted_mycte.quote_style = Some(openlineage_sql::QuoteStyle {
        database: None,
        schema: None,
        name: Some('"'),
    });
    assert_eq!(
        test_sql_dialect(
            "WITH MyCte AS (SELECT * FROM unquoted_src), \
             other AS (SELECT * FROM \"mycte\") SELECT * FROM other",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![quoted_mycte],
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn quoted_reference_matching_unquoted_cte_spelling_is_not_the_cte() {
    // In Snowflake, "mycte" is not the CTE mycte (stored MYCTE): the
    // CTE lookup misses - t1 is not collected - and the reference is
    // the real table it names, collected as an input. (This pinned
    // the old alias-table limitation, disclosed on the PR, until
    // #5178 made non-CTE references ignore the CTE placeholder
    // aliases; the expectation now states the fixed behavior.)
    let mut quoted_mycte = table("mycte");
    quoted_mycte.quote_style = Some(openlineage_sql::QuoteStyle {
        database: None,
        schema: None,
        name: Some('"'),
    });
    assert_eq!(
        test_sql_dialect(
            "WITH mycte AS (SELECT * FROM t1) SELECT * FROM \"mycte\"",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![quoted_mycte],
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn unquoted_reference_matching_quoted_cte_spelling_is_not_the_cte() {
    // Mirrored: mycte folds to MYCTE in Snowflake, which is not the
    // quoted CTE "mycte"; the lookup misses and the reference is
    // collected as the real table it names (see the test above for
    // the history of this expectation).
    assert_eq!(
        test_sql_dialect(
            "WITH \"mycte\" AS (SELECT * FROM t1) SELECT * FROM mycte",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["mycte"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn delimited_case_insensitive_cte_references_resolve() {
    // In SQL Server and SQLite, delimited identifiers stay
    // case-insensitive, so a delimited reference in different case
    // names the same CTE. (MySQL is not here: under the folding
    // rules #5178 settled on, it compares names exactly, as it does
    // table aliases under its default configuration.)
    for (sql, dialect) in [
        (
            "WITH [MyCte] AS (SELECT * FROM a) SELECT * FROM [mycte]",
            "mssql",
        ),
        (
            "WITH \"MyCte\" AS (SELECT * FROM a) SELECT * FROM \"mycte\"",
            "sqlite",
        ),
    ] {
        assert_eq!(
            test_sql_dialect(sql, dialect).unwrap().table_lineage,
            TableLineage {
                in_tables: tables(vec!["a"]),
                out_tables: tables(vec![])
            }
        );
    }
    // Through a sibling CTE's body, too.
    assert_eq!(
        test_sql_dialect(
            "WITH [MyCte] AS (SELECT * FROM a), \
             other AS (SELECT * FROM [mycte]) SELECT * FROM other",
            "mssql",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn recursive_cte_with_delimited_case_differing_self_reference() {
    // SQL Server lets any CTE refer to itself; a delimited
    // self-reference in different case is still the recursion, not a
    // phantom table input.
    assert_eq!(
        test_sql_dialect(
            "WITH MyCte AS (SELECT n FROM a UNION ALL SELECT n FROM [mycte]) \
             SELECT * FROM MyCte",
            "mssql",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec![])
        }
    );
}
#[test]
fn quoted_reference_equal_to_unquoted_fold_resolves() {
    // A quoted reference whose spelling is exactly the stored form of
    // an unquoted definition names that CTE (Postgres folds MyCte to
    // mycte; Snowflake folds it to MYCTE), and vice versa.
    for (sql, dialect) in [
        (
            "WITH MyCte AS (SELECT * FROM a) SELECT * FROM \"mycte\"",
            "postgres",
        ),
        (
            "WITH \"mycte\" AS (SELECT * FROM a) SELECT * FROM MyCte",
            "postgres",
        ),
        (
            "WITH MyCte AS (SELECT * FROM a) SELECT * FROM \"MYCTE\"",
            "snowflake",
        ),
    ] {
        assert_eq!(
            test_sql_dialect(sql, dialect).unwrap().table_lineage,
            TableLineage {
                in_tables: tables(vec!["a"]),
                out_tables: tables(vec![])
            }
        );
    }
}

#[test]
fn non_recursive_cte_in_derived_table_reads_table_with_its_own_name() {
    // The WITH sits inside the derived table's scope: the CTE body
    // reads the real table `orders` (a non-recursive CTE's own name
    // is not in scope in its body), and that input must survive the
    // subquery's CTE resolution.
    assert_eq!(
        test_sql_dialect(
            "SELECT * FROM (WITH orders AS (SELECT * FROM orders) SELECT * FROM orders) x",
            "postgres",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["orders"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn non_recursive_cte_in_insert_source_reads_table_with_its_own_name() {
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO target WITH orders AS (SELECT * FROM orders) SELECT * FROM orders",
            "postgres",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["orders"]),
            out_tables: tables(vec!["target"])
        }
    );
}

#[test]
fn inner_cte_reading_outer_cte_with_its_own_name_keeps_outer_inputs() {
    // The inner CTE's body reads `orders`, which there names the
    // OUTER CTE: the outer CTE is used, so its input `raw` is kept.
    assert_eq!(
        test_sql_dialect(
            "WITH orders AS (SELECT * FROM raw) \
             SELECT * FROM (WITH orders AS (SELECT * FROM orders) SELECT * FROM orders) x",
            "postgres",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["raw"]),
            out_tables: tables(vec![])
        }
    );
}
