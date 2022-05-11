extern crate core;

use openlineage_sql::{parse_sql, SqlMeta};
use sqlparser::dialect::PostgreSqlDialect;
use std::sync::Arc;

#[macro_use]
mod test_utils;
use test_utils::*;

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
        ),
        SqlMeta {
            in_tables: table("transactions"),
            out_tables: table("potential_fraud")
        }
    );
}

#[test]
fn parse_bugged_cte() {
    assert_eq!(
        parse_sql(
            "
                WITH sum_trans (
                    SELECT user_id, COUNT(*) as cnt, SUM(amount) as balance
                    FROM transactions
                    WHERE created_date > '2020-01-01'
                    GROUP BY user_id
                )
                INSERT INTO potential_fraud (user_id, cnt, balance)
                SELECT user_id, cnt, balance
                FROM sum_trans
                WHERE count > 1000 OR balance > 100000;",
            Arc::new(PostgreSqlDialect {}),
            None
        )
        .unwrap_err(),
        "sql parser error: Expected ), found: user_id"
    )
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
        ),
        SqlMeta {
            in_tables: table("employees"),
            out_tables: table("sub_employees")
        }
    )
}

#[test]
fn multiple_ctes() {
    assert_eq!(
        test_sql("
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
        "), SqlMeta {
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
        test_sql(
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
        "
        ),
        SqlMeta {
            in_tables: table("d_n.f_p_s"),
            out_tables: table("dev_d_n.f_p_s_m")
        }
    )
}
