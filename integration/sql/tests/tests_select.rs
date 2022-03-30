use openlineage_sql::{parse_sql, BigQueryDialect, SqlMeta};

#[macro_use]
mod test_utils;
use test_utils::*;

#[test]
fn select_simple() {
    assert_eq!(
        test_sql("SELECT * FROM table0;",),
        SqlMeta {
            in_tables: table("table0"),
            out_tables: vec![]
        }
    )
}
#[test]
fn select_from_schema_table() {
    assert_eq!(
        test_sql("SELECT * FROM schema0.table0;",),
        SqlMeta {
            in_tables: table("schema0.table0"),
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
        ),
        SqlMeta {
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
        ),
        SqlMeta {
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
        ),
        SqlMeta {
            in_tables: tables(vec!["table0", "table1"]),
            out_tables: vec![]
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
            ",
            Box::new(BigQueryDialect),
            None
        )
        .unwrap(),
        SqlMeta {
            in_tables: table("random-project.dbt_test1.source_table"),
            out_tables: vec![]
        }
    )
}

#[test]
fn select_into() {
    assert_eq!(
        test_sql("SELECT * INTO table0 FROM table1;",),
        SqlMeta {
            in_tables: table("table1"),
            out_tables: table("table0")
        }
    )
}
