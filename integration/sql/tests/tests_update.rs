use openlineage_sql::SqlMeta;

#[macro_use]
mod test_utils;
use test_utils::*;

#[test]
fn update_table() {
    assert_eq!(
        test_sql("UPDATE table0 SET col0 = val0 WHERE col1 = val1"),
        SqlMeta {
            in_tables: vec![],
            out_tables: table("table0")
        }
    );
}

#[test]
fn update_table_from() {
    assert_eq!(
        test_sql(
            "UPDATE dataset.Inventory i
            SET quantity = i.quantity + n.quantity,
                supply_constrained = false
            FROM dataset.NewArrivals n
            WHERE i.product = n.product"
        ),
        SqlMeta {
            in_tables: table("dataset.NewArrivals"),
            out_tables: table("dataset.Inventory")
        }
    )
}

#[test]
fn update_table_from_subquery() {
    assert_eq!(
        test_sql(
            "UPDATE dataset.Inventory
            SET quantity = quantity +
            (SELECT quantity FROM dataset.NewArrivals
            WHERE Inventory.product = NewArrivals.product),
            supply_constrained = false
            WHERE product IN (SELECT product FROM dataset.NewArrivals)"
        ),
        SqlMeta {
            in_tables: table("dataset.NewArrivals"),
            out_tables: table("dataset.Inventory")
        }
    )
}
