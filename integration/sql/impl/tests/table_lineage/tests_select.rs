// Copyright 2018-2024 contributors to the OpenLineage project
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
            let sql = format!("SELECT col1 FROM identifier({}) WHERE x = 1;", in_table_id);
            assert_eq!(
                test_sql_dialect(&sql, dialect).unwrap().table_lineage,
                TableLineage {
                    in_tables: in_tables.clone(),
                    out_tables: vec![],
                },
                "Failed for dialect: {} with SQL: {}",
                dialect,
                sql
            );
        }
    }
}
