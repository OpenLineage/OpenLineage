use openlineage_sql::{parse_sql, SqlMeta};
use sqlparser::dialect::PostgreSqlDialect;

#[macro_use]
mod test_utils;
use test_utils::*;

#[test]
fn insert_values() {
    assert_eq!(
        test_sql("INSERT INTO TEST VALUES(1)",),
        SqlMeta {
            in_tables: vec![],
            out_tables: table("TEST")
        }
    );
}

#[test]
fn insert_cols_values() {
    assert_eq!(
        test_sql("INSERT INTO tbl(col1, col2) VALUES (1, 2), (2, 3)",),
        SqlMeta {
            in_tables: vec![],
            out_tables: table("tbl")
        }
    );
}

#[test]
fn insert_select_table() {
    assert_eq!(
        test_sql("INSERT INTO TEST SELECT * FROM TEMP",),
        SqlMeta {
            in_tables: table("TEMP"),
            out_tables: table("TEST")
        }
    );
}

#[test]
fn insert_nested_select() {
    assert_eq!(test_sql("
                INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)
                SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
                    order_placed_on,
                    COUNT(*) AS orders_placed
                FROM top_delivery_times
                GROUP BY order_placed_on;
            ",
    ), SqlMeta {
        in_tables: table("top_delivery_times"),
        out_tables: table("popular_orders_day_of_week")
    })
}
