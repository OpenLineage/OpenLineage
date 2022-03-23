use openlineage_sql::{parse_sql, SqlMeta};

#[test]
fn parse_simple_cte() {
    assert_eq!(
        parse_sql(
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
                "
        )
        .unwrap(),
        SqlMeta {
            in_tables: vec![String::from("transactions")],
            out_tables: Some(String::from("potential_fraud"))
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
                WHERE count > 1000 OR balance > 100000;"
        )
        .unwrap_err(),
        "sql parser error: Expected ), found: user_id"
    )
}

#[test]
fn parse_recursive_cte() {
    assert_eq!(
        parse_sql(
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
        .unwrap(),
        SqlMeta {
            in_tables: vec![String::from("employees")],
            out_tables: Some(String::from("sub_employees"))
        }
    )
}

#[test]
fn multiple_ctes() {
    assert_eq!(
        parse_sql(
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
        .unwrap(),
        SqlMeta {
            in_tables: vec![
                String::from("DEMO_DB.public.stg_customers"),
                String::from("DEMO_DB.public.stg_orders")
            ],
            out_tables: None
        }
    )
}
