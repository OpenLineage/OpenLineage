// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::TableLineage;

#[test]
fn select_simple() {
    assert_eq!(
        test_sql("SELECT * FROM table0;",).unwrap().table_lineage,
        TableLineage {
            in_tables: tables(vec!["table0"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_where_exists_subquery() {
    assert_eq!(
        test_sql("SELECT 1 WHERE EXISTS (SELECT 1 FROM customers);")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["customers"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_having_exists_subquery() {
    assert_eq!(
        test_sql(
            "SELECT count(*)
             FROM orders
             HAVING EXISTS (SELECT 1 FROM customers);"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["customers", "orders"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_qualify_exists_subquery() {
    assert_eq!(
        test_sql_dialect(
            "SELECT row_number() OVER (ORDER BY id)
             FROM orders
             QUALIFY EXISTS (SELECT 1 FROM customers);",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["customers", "orders"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_aggregate_filter_subquery() {
    assert_eq!(
        test_sql(
            "SELECT count(*) FILTER (
                 WHERE EXISTS (SELECT 1 FROM customers)
             ) FROM orders;"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["customers", "orders"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_from_schema_table() {
    assert_eq!(
        test_sql("SELECT * FROM schema0.table0;",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["schema0.table0"]),
            out_tables: vec![]
        }
    )
}
#[test]
fn select_join() {
    assert_eq!(
        test_sql(
            "
                SELECT col0, col1, col2
                FROM table0
                JOIN table1
                ON t1.col0 = t2.col0",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["table0", "table1"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_parenthesized_join() {
    for sql in [
        "SELECT * FROM (foo JOIN bar ON foo.id = bar.id)",
        "SELECT * FROM (foo JOIN bar ON foo.id = bar.id) AS joined",
    ] {
        assert_eq!(
            test_sql(sql).unwrap().table_lineage,
            TableLineage {
                in_tables: tables(vec!["bar", "foo"]),
                out_tables: vec![]
            }
        )
    }
}

#[test]
fn select_join_condition_subquery() {
    assert_eq!(
        test_sql(
            "SELECT a.id
             FROM a
             JOIN b ON b.id IN (SELECT c.id FROM c)"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a", "b", "c"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_inner_join() {
    assert_eq!(
        test_sql(
            "
                SELECT col0, col1, col2
                FROM table0
                INNER JOIN table1
                ON t1.col0 = t2.col0",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["table0", "table1"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_left_join() {
    assert_eq!(
        test_sql(
            "
            SELECT col0, col1, col2
            FROM table0
            LEFT JOIN table1
            ON t1.col0 = t2.col0",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["table0", "table1"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_bigquery_escaping() {
    assert_eq!(
        test_sql_dialect(
            "
            SELECT *
            FROM `random-project`.`dbt_test1`.`source_table`
            WHERE id = 1
            ",
            "bigquery"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["`random-project`.`dbt_test1`.`source_table`"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_into() {
    assert_eq!(
        test_sql("SELECT * INTO table0 FROM table1;",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["table1"]),
            out_tables: tables(vec!["table0"])
        }
    )
}

#[test]
fn select_redshift() {
    assert_eq!(
        test_sql_dialect("SELECT [col1] FROM [test_schema].[test_table]", "redshift")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["[test_schema].[test_table]"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_sqlserver() {
    assert_eq!(
        test_sql_dialect("SELECT [col1] FROM [test_schema].[test_table]", "sqlserver")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["[test_schema].[test_table]"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_with_table_generator() {
    assert_eq!(
        test_sql(
            "
            SELECT row_number() OVER (ORDER BY col) row_num, d
            FROM TABLE(GENERATOR(ROWCOUNT => (12 * 6)))
            JOIN test_schema.test_table ON test_table.d = row_number()
            "
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["test_schema.test_table"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_bigquery_unnest_generate_array() {
    // Regression test for the panic reported at
    // https://github.com/OpenLineage/OpenLineage/issues/1358 for Snowflake's
    // TABLE(GENERATOR(...)), applied to BigQuery's UNNEST(GENERATE_ARRAY(...))
    // table factor, which was still unhandled: `TableFactor::UNNEST` fell
    // through to the catch-all `Err` arm in `Visit for TableFactor`.
    assert_eq!(
        test_sql_dialect(
            "
            SELECT test_table.col0, n
            FROM test_schema.test_table
            CROSS JOIN UNNEST(GENERATE_ARRAY(1, 1000)) AS n
            ",
            "bigquery"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["test_schema.test_table"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_bigquery_unnest_literal_array() {
    assert_eq!(
        test_sql_dialect("SELECT * FROM UNNEST([1, 2, 3]) AS n", "bigquery")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: vec![]
        }
    )
}

#[test]
fn select_bigquery_unnest_with_offset() {
    assert_eq!(
        test_sql_dialect(
            "
            SELECT test_table.col0, n, offset
            FROM test_schema.test_table
            CROSS JOIN UNNEST(GENERATE_ARRAY(1, 1000)) AS n WITH OFFSET AS offset
            ",
            "bigquery"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["test_schema.test_table"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_bigquery_unnest_array_subquery() {
    // UNNEST's own array expression can reference a real table (e.g. a subquery
    // wrapped in ARRAY(...)) - that shouldn't be silently discarded.
    assert_eq!(
        test_sql_dialect(
            "SELECT n FROM UNNEST(ARRAY(SELECT id FROM source_table)) AS n",
            "bigquery"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["source_table"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_bigquery_unpivot() {
    // Regression test for the same TableFactor::Unpivot gap as UNNEST above -
    // this variant had no arm and fell through to the catch-all `Err`.
    assert_eq!(
        test_sql_dialect(
            "
            SELECT *
            FROM test_schema.test_table
            UNPIVOT(sales FOR quarter IN (q1_sales, q2_sales, q3_sales))
            ",
            "bigquery"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["test_schema.test_table"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_bigquery_unpivot_with_alias() {
    assert_eq!(
        test_sql_dialect(
            "
            SELECT unpivoted.quarter, unpivoted.sales
            FROM test_schema.test_table
            UNPIVOT(sales FOR quarter IN (q1_sales AS 'Q1', q2_sales AS 'Q2')) AS unpivoted
            ",
            "bigquery"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["test_schema.test_table"]),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_window_function() {
    assert_eq!(
        test_sql(
            "SELECT row_number() OVER (ORDER BY dt DESC), \
               sum(foo) OVER (PARTITION BY a, b ORDER BY c, d \
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
               avg(bar) OVER (ORDER BY a \
               RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), \
               sum(bar) OVER (ORDER BY a \
               RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND INTERVAL '1 MONTH' FOLLOWING), \
               COUNT(*) OVER (ORDER BY a \
               RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND INTERVAL '1 DAY' FOLLOWING), \
               max(baz) OVER (ORDER BY a \
               ROWS UNBOUNDED PRECEDING), \
               sum(qux) OVER (ORDER BY a \
               GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
               FROM foo"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![table("foo")],
            out_tables: vec![],
        }
    )
}

#[test]
fn select_bq_array_function() {
    assert_eq!(
        test_sql("SELECT l.LOCATION[offset(0)] AS my_city FROM my_bq_dataset.my_table_2 AS l")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: vec![table("my_bq_dataset.my_table_2")],
            out_tables: vec![],
        }
    )
}

#[test]
fn select_bq_array_subquery() {
    assert_eq!(
        test_sql_dialect("SELECT ARRAY(SELECT id FROM source_table)", "bigquery")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: vec![table("source_table")],
            out_tables: vec![],
        }
    )
}

#[test]
fn select_identifier_function() {
    let test_cases = vec![
        ("target", vec![table("target")]),
        ("'target'", vec![table("target")]),
        ("$target", vec![]),
        (":target", vec![]),
        ("?", vec![]),
        (":myschema || '.' || :mytab", vec![]),
    ];

    let dialects = vec!["snowflake", "databricks", "mssql"];

    for (in_table_id, in_tables) in &test_cases {
        for dialect in &dialects {
            let sql = format!("SELECT col1 FROM identifier({in_table_id}) WHERE x = 1;");
            assert_eq!(
                test_sql_dialect(&sql, dialect).unwrap().table_lineage,
                TableLineage {
                    in_tables: in_tables.clone(),
                    out_tables: vec![],
                },
                "Failed for dialect: {dialect} with SQL: {sql}"
            );
        }
    }
}

#[test]
fn select_snowflake_lateral() {
    assert_eq!(
        test_sql_dialect(
            "SELECT id as ID,
        f.value AS Contact,
        f1.value:type AS Type,
        f1.value:content AS Details
        FROM my_snowflake_schema.persons p,
        lateral flatten(input => p.c, path => 'contact') f,
        lateral flatten(input => f.value:business) f1;",
            "snowflake"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![table("my_snowflake_schema.persons")],
            out_tables: vec![],
        }
    )
}
