// Copyright 2018-2025 contributors to the OpenLineage project
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

#[test]
fn update_identifier_function() {
    let test_cases = vec![
        (
            "target",
            "'source'",
            vec![table("source")],
            vec![table("target")],
        ),
        ("$target", ":source", vec![], vec![]),
        ("?", "$source", vec![], vec![]),
        (
            ":myschema || '.' || :mytab",
            "source",
            vec![table("source")],
            vec![],
        ),
    ];

    let dialects = vec!["snowflake", "mssql"]; // Databricks has different update syntax

    for (out_table_id, in_table_id, in_tables, out_tables) in &test_cases {
        for dialect in &dialects {
            let sql = format!(
                "UPDATE \
                IDENTIFIER({out_table_id}) \
                SET v = src.v \
                FROM \
                identifier({in_table_id}) as src \
                WHERE target.k = src.k;"
            );
            assert_eq!(
                test_sql_dialect(&sql, dialect).unwrap().table_lineage,
                TableLineage {
                    in_tables: in_tables.clone(),
                    out_tables: out_tables.clone(),
                },
                "Failed for dialect: {dialect} with SQL: {sql}"
            );
        }
    }
}

#[test]
fn update_identifier_function_databricks() {
    let test_cases = vec![
        (
            "target",
            "source",
            vec![table("source")],
            vec![table("target")],
        ),
        ("$target", ":source", vec![], vec![]),
        ("?", "$source", vec![], vec![]),
        (
            ":myschema || '.' || :mytab",
            "source",
            vec![table("source")],
            vec![],
        ),
    ];

    for (out_table_id, in_table_id, in_tables, out_tables) in &test_cases {
        let sql = format!(
            "UPDATE \
            IDENTIFIER({out_table_id}) \
            SET v = src.v \
            WHERE EXISTS (SELECT x FROM identifier({in_table_id}) WHERE t1.oid = oid);"
        );
        assert_eq!(
            test_sql_dialect(&sql, "databricks").unwrap().table_lineage,
            TableLineage {
                in_tables: in_tables.clone(),
                out_tables: out_tables.clone(),
            },
            "Failed for dialect: databricks with SQL: {sql}",
        );
    }
}
