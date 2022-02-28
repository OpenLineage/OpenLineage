use std::collections::HashSet;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor, With};
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;

#[derive(Debug, PartialEq)]
struct Context {
    ctes: HashSet<String>,
    inputs: HashSet<String>,
    output: Option<String>,
}

impl Context {
    fn new() -> Context {
        Context {
            ctes: HashSet::new(),
            inputs: HashSet::new(),
            output: None,
        }
    }

    fn add_input(&mut self, table: &String) {
        if !self.ctes.contains(table) {
            self.inputs.insert(table.clone());
        }
    }

    fn set_output(&mut self, output: &String) {
        self.output = Some(output.clone());
    }
}

#[pyclass]
#[derive(Debug, PartialEq)]
struct QueryMetadata {
    inputs: Vec<String>,
    output: Option<String>,
}

impl From<Context> for QueryMetadata {
    fn from(ctx: Context) -> Self {
        let mut inputs: Vec<String> = ctx.inputs.into_iter().collect();
        inputs.sort();
        QueryMetadata {
            inputs: inputs,
            output: ctx.output,
        }
    }
}

fn parse_with(with: &With, context: &mut Context) -> Result<(), String> {
    for cte in &with.cte_tables {
        context.ctes.insert(cte.alias.name.value.clone());
        parse_query(&cte.query, context)?;
    }
    Ok(())
}

fn parse_select(select: &Select, context: &mut Context) -> Result<(), String> {
    for table in &select.from {
        match &table.relation {
            TableFactor::Table { name, .. } => {
                context.add_input(&name.to_string());
            }
            _ => {
                return Err(String::from(
                    "TableFactor other than straight table not implemented",
                ))
            }
        }
        for join in &table.joins {
            match &join.relation {
                TableFactor::Table { name, .. } => {
                    context.add_input(&name.to_string());
                }
                _ => {
                    return Err(String::from(
                        "TableFactor other than straight table not implemented",
                    ))
                }
            }
        }
    }
    Ok(())
}

fn parse_setexpr(setexpr: &SetExpr, context: &mut Context) -> Result<(), String> {
    match setexpr {
        SetExpr::Select(select) => parse_select(&select, context)?,
        SetExpr::Values(_) => (),
        SetExpr::Insert(stmt) => parse_stmt(stmt, context)?,
        SetExpr::Query(q) => parse_query(q, context)?,
        SetExpr::SetOperation{op, all, left, right} => {
            parse_setexpr(&left, context)?;
            parse_setexpr(&right, context)?;
        }
    };
    Ok(())
}

fn parse_query(query: &Query, context: &mut Context) -> Result<(), String> {
    match &query.with {
        Some(with) => parse_with(&with, context)?,
        None => (),
    };

    parse_setexpr(&query.body, context)?;
    Ok(())
}

fn parse_stmt(stmt: &Statement, context: &mut Context) -> Result<(), String> {
    match stmt {
        Statement::Query(query) => {
            parse_query(query, context)?;
            Ok(())
        }
        Statement::Insert {
            or: _,
            table_name,
            columns: _,
            overwrite: _,
            source,
            partitioned: _,
            after_columns: _,
            table: _,
            on: _,
        } => {
            println!("Tabelka {}", table_name);
            parse_query(source, context)?;
            context.set_output(&table_name.to_string());
            Ok(())
        }
        _ => Err(String::from("not a insert")),
    }
}

fn parse_sql(sql: &str) -> Result<QueryMetadata, String> {
    let dialect = SnowflakeDialect;
    let ast = match Parser::parse_sql(&dialect, sql) {
        Ok(k) => k,
        Err(e) => return Err(e.to_string().to_owned()),
    };
    println!("AST: {:?}", ast);

    if ast.is_empty() {
        return Err(String::from("Empty statement list"));
    }

    let mut context = Context::new();
    let stmt = ast.first();

    parse_stmt(stmt.unwrap(), &mut context)?;
    Ok(QueryMetadata::from(context))
}

// Parses SQL.
#[pyfunction]
fn parse(sql: &str) -> PyResult<QueryMetadata> {
    match parse_sql(sql) {
        Ok(ok) => Ok(ok),
        Err(err) => Err(PyRuntimeError::new_err(err)),
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Query;

    use crate::{parse_sql, QueryMetadata};

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
            parse_sql("
                SELECT col0, col1, col2 
                FROM table0 
                JOIN table1 
                ON t1.col0 = t2.col0"
            ).unwrap(),
            QueryMetadata {
                inputs: vec![String::from("table0"), String::from("table1")],
                output: None
            }
        )
    }

    #[test]
    fn select_inner_join() {
        assert_eq!(
            parse_sql("
                SELECT col0, col1, col2
                FROM table0
                INNER JOIN table1
                ON t1.col0 = t2.col0"
            ).unwrap(),
            QueryMetadata {
                inputs: vec![String::from("table0"), String::from("table1")],
                output: None
            }
        )
    }

    #[test]
    fn select_left_join() {
        assert_eq!(
            parse_sql("
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
    fn test_parse_simple_cte() {
        assert_eq!(
            parse_sql("
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
            ).unwrap(),
            QueryMetadata {
                inputs: vec![String::from("transactions")],
                output: Some(String::from("potential_fraud"))
            }
        );
    }

    #[test]
    fn test_parse_bugged_cte() {
        assert_eq!(
            parse_sql("
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
            ).unwrap_err(),
            "sql parser error: Expected ), found: user_id"
        )
    }

    #[test]
    fn test_parse_recursive_cte() {
        assert_eq!(parse_sql("
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
        ").unwrap(), QueryMetadata{
            inputs: vec![String::from("employees")],
            output: Some(String::from("sub_employees"))
        })
    }

    #[test]
    fn test_multiple_ctes() {
        assert_eq!(parse_sql("
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
        ").unwrap(), QueryMetadata {
            inputs: vec![String::from("DEMO_DB.public.stg_customers"), String::from("DEMO_DB.public.stg_orders")],
            output: None
        })
    }

    // #[test]
    // fn merge_subquery_when_not_matched() {
    //     assert_eq!(parse_sql("
    //     MERGE INTO s.bar as dest
    //     USING (
    //         SELECT *
    //         FROM
    //         s.foo
    //     ) as stg
    //     ON dest.D = stg.D
    //       AND dest.E = stg.E
    //     WHEN NOT MATCHED THEN
    //     INSERT (
    //       A,
    //       B,
    //       C)
    //     VALUES
    //     (
    //         stg.A
    //         ,stg.B
    //         ,stg.C
    //     )").unwrap(), QueryMetadata{
    //         inputs: vec![String::from("s.foo")],
    //         output: Some(String::from("s.bar"))
    //     });
    // }

    // #[test]
    // fn test_tpcds_cte_query() {
    //     assert_eq!(parse_sql("
    //     WITH year_total AS
    //         (SELECT c_customer_id customer_id,
    //                 c_first_name customer_first_name,
    //                 c_last_name customer_last_name,
    //                 c_preferred_cust_flag customer_preferred_cust_flag,
    //                 c_birth_country customer_birth_country,
    //                 c_login customer_login,
    //                 c_email_address customer_email_address,
    //                 d_year dyear,
    //                 Sum(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt)
    //                     + ss_ext_sales_price) / 2) year_total,
    //                 's' sale_type
    //         FROM src.customer,
    //             store_sales,
    //             date_dim
    //         WHERE c_customer_sk = ss_customer_sk
    //             AND ss_sold_date_sk = d_date_sk GROUP  BY c_customer_id,
    //                                                     c_first_name,
    //                                                     c_last_name,
    //                                                     c_preferred_cust_flag,
    //                                                     c_birth_country,
    //                                                     c_login,
    //                                                     c_email_address,
    //                                                     d_year)
    //     SELECT t_s_secyear.customer_id,
    //         t_s_secyear.customer_first_name,
    //         t_s_secyear.customer_last_name,
    //         t_s_secyear.customer_preferred_cust_flag
    //     FROM year_total t_s_firstyear,
    //         year_total t_s_secyear,
    //         year_total t_c_firstyear,
    //         year_total t_c_secyear,
    //         year_total t_w_firstyear,
    //         year_total t_w_secyear
    //     WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
    //         AND t_s_firstyear.customer_id = t_c_secyear.customer_id
    //         AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
    //         AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
    //         AND t_s_firstyear.customer_id = t_w_secyear.customer_id
    //         AND t_s_firstyear.sale_type = 's'
    //         AND t_c_firstyear.sale_type = 'c'
    //         AND t_w_firstyear.sale_type = 'w'
    //         AND t_s_secyear.sale_type = 's'
    //         AND t_c_secyear.sale_type = 'c'
    //         AND t_w_secyear.sale_type = 'w'
    //         AND t_s_firstyear.dyear = 2001
    //         AND t_s_secyear.dyear = 2001 + 1
    //         AND t_c_firstyear.dyear = 2001
    //         AND t_c_secyear.dyear = 2001 + 1
    //         AND t_w_firstyear.dyear = 2001
    //         AND t_w_secyear.dyear = 2001 + 1
    //         AND t_s_firstyear.year_total > 0
    //         AND t_c_firstyear.year_total > 0
    //         AND t_w_firstyear.year_total > 0
    //         AND CASE WHEN
    //                 t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
    //                 ELSE NULL
    //             END > CASE WHEN
    //                 t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
    //                 ELSE NULL
    //             END
    //         AND CASE WHEN
    //                 t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
    //                 ELSE NULL
    //             END > CASE WHEN
    //                 t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
    //                 ELSE NULL
    //             END
    //         ORDER  BY t_s_secyear.customer_id,
    //                 t_s_secyear.customer_first_name,
    //                 t_s_secyear.customer_last_name,
    //                 t_s_secyear.customer_preferred_cust_flag
    //     LIMIT 100;
    //     ").unwrap(), QueryMetadata{
    //         inputs: vec![],
    //         output: None
    //     })
    // }
}
