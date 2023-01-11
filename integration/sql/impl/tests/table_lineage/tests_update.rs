// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::TableLineage;

#[test]
fn update_table() {
    assert_eq!(
        test_sql("UPDATE table0 SET col0 = val0 WHERE col1 = val1")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["table0"])
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
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["dataset.NewArrivals"]),
            out_tables: tables(vec!["dataset.Inventory"])
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
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["dataset.NewArrivals"]),
            out_tables: tables(vec!["dataset.Inventory"])
        }
    )
}
