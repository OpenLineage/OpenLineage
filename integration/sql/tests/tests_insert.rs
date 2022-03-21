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
