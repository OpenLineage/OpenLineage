// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::{parse_sql, ExtractionError, TableLineage};
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
