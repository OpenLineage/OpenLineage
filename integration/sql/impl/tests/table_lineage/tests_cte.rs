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

// A CTE is visible only to the query it is attached to, so a write target
// spelled like a CTE still names a table.
#[test]
fn write_target_spelled_like_a_cte_is_an_output() {
    for (sql, dialect) in [
        (
            "WITH c AS (SELECT id FROM src) INSERT INTO c SELECT id FROM c",
            "postgres",
        ),
        (
            "WITH \"c\" AS (SELECT id FROM src) INSERT INTO c SELECT id FROM \"c\"",
            "snowflake",
        ),
        (
            "WITH c AS (SELECT id FROM src) INSERT INTO c SELECT id FROM c",
            "snowflake",
        ),
        (
            "WITH c AS (SELECT id FROM src) DELETE FROM c WHERE id IN (SELECT id FROM c)",
            "postgres",
        ),
    ] {
        assert_eq!(
            cte_lineage(sql, dialect),
            TableLineage {
                in_tables: tables(vec!["src"]),
                out_tables: tables(vec!["c"])
            },
            "{dialect}: {sql}"
        );
    }
}

// `TABLE c` reads CTE `c` like `FROM c` does, so `c` is not an input.
#[test]
fn table_statement_naming_a_cte_reads_the_cte() {
    assert_eq!(
        in_tables(
            "WITH c AS (SELECT a FROM src) SELECT a FROM c UNION ALL TABLE c",
            "postgres"
        ),
        vec!["src"]
    );
    assert_eq!(
        in_tables(
            "WITH c AS (SELECT a FROM src) SELECT a FROM other UNION ALL TABLE c",
            "postgres"
        ),
        vec!["other", "src"]
    );
}
