use openlineage_sql::{parse_sql, SqlMeta};
use sqlparser::dialect::PostgreSqlDialect;

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
            Box::new(PostgreSqlDialect {}),
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
        ),
        SqlMeta {
            in_tables: tables(vec![
                "DEMO_DB.public.stg_customers",
                "DEMO_DB.public.stg_orders"
            ]),
            out_tables: vec![]
        }
    )
}
