use openlineage_sql::{parse_sql, QueryMetadata};

#[test]
fn insert_values() {
    assert_eq!(
        parse_sql("INSERT INTO TEST VALUES(1)").unwrap(),
        QueryMetadata {
            inputs: vec![],
            output: Some(String::from("TEST"))
        }
    );
}

#[test]
fn insert_cols_values() {
    assert_eq!(
        parse_sql("INSERT INTO tbl(col1, col2) VALUES (1, 2), (2, 3)").unwrap(),
        QueryMetadata {
            inputs: vec![],
            output: Some(String::from("tbl"))
        }
    );
}

#[test]
fn insert_select_table() {
    assert_eq!(
        parse_sql("INSERT INTO TEST SELECT * FROM TEMP").unwrap(),
        QueryMetadata {
            inputs: vec![String::from("TEMP")],
            output: Some(String::from("TEST"))
        }
    );
}

#[test]
fn drop_errors() {
    assert_eq!(
        parse_sql("DROP TABLE TEST"),
        Err(String::from("not a insert"))
    );
}

#[test]
fn select_simple() {
    assert_eq!(
        parse_sql("SELECT * FROM table0;").unwrap(),
        QueryMetadata {
            inputs: vec![String::from("table0")],
            output: None
        }
    )
}
#[test]
fn select_from_schema_table() {
    assert_eq!(
        parse_sql("SELECT * FROM schema0.table0;").unwrap(),
        QueryMetadata {
            inputs: vec![String::from("schema0.table0")],
            output: None
        }
    )
}
#[test]
fn select_join() {
    assert_eq!(
        parse_sql(
            "
                SELECT col0, col1, col2
                FROM table0
                JOIN table1
                ON t1.col0 = t2.col0"
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![String::from("table0"), String::from("table1")],
            output: None
        }
    )
}

#[test]
fn select_inner_join() {
    assert_eq!(
        parse_sql(
            "
                SELECT col0, col1, col2
                FROM table0
                INNER JOIN table1
                ON t1.col0 = t2.col0"
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![String::from("table0"), String::from("table1")],
            output: None
        }
    )
}

#[test]
fn select_left_join() {
    assert_eq!(
        parse_sql(
            "
            SELECT col0, col1, col2
            FROM table0
            LEFT JOIN table1
            ON t1.col0 = t2.col0"
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![String::from("table0"), String::from("table1")],
            output: None
        }
    )
}

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
        QueryMetadata {
            inputs: vec![String::from("transactions")],
            output: Some(String::from("potential_fraud"))
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
        QueryMetadata {
            inputs: vec![String::from("employees")],
            output: Some(String::from("sub_employees"))
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
        QueryMetadata {
            inputs: vec![
                String::from("DEMO_DB.public.stg_customers"),
                String::from("DEMO_DB.public.stg_orders")
            ],
            output: None
        }
    )
}

#[test]
fn select_bigquery_excaping() {
    assert_eq!(
        parse_sql(
            "
            SELECT *
            FROM `random-project`.`dbt_test1`.`source_table`
            WHERE id = 1
        "
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![String::from("`random-project`.`dbt_test1`.`source_table`")],
            output: None
        }
    )
}

#[test]
fn insert_nested_select() {
    assert_eq!(parse_sql("
            INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)
            SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
                order_placed_on,
                COUNT(*) AS orders_placed
            FROM top_delivery_times
            GROUP BY order_placed_on;
        ").unwrap(), QueryMetadata {
        inputs: vec![String::from("top_delivery_times")],
        output: Some(String::from("popular_orders_day_of_week"))
    })
}

#[test]
fn merge_subquery_when_not_matched() {
    assert_eq!(
        parse_sql(
            "
        MERGE INTO s.bar as dest
        USING (
            SELECT *
            FROM
            s.foo
        ) as stg
        ON dest.D = stg.D
          AND dest.E = stg.E
        WHEN NOT MATCHED THEN
        INSERT (
          A,
          B,
          C)
        VALUES
        (
            stg.A
            ,stg.B
            ,stg.C
        )"
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![String::from("s.foo")],
            output: Some(String::from("s.bar"))
        }
    );
}
