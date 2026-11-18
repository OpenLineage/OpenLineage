// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql, test_sql_dialect};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_simple_cte() {
    let output = test_sql(
        "WITH my_cte AS (
            SELECT a,b,c FROM t1
         )
         SELECT a,c FROM my_cte",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "c".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t1")),
                    name: "c".to_string()
                }]
            },
        ]
    );
}

fn column(name: &str, origin: &str) -> ColumnLineage {
    ColumnLineage {
        descendant: ColumnMeta {
            origin: None,
            name: name.to_string(),
        },
        lineage: vec![ColumnMeta {
            origin: Some(table(origin)),
            name: name.to_string(),
        }],
    }
}

#[test]
fn test_cte_select_wildcard_from_table() {
    let output = test_sql(
        "WITH source AS (SELECT * FROM raw_customers WHERE active)
         SELECT id, name FROM source",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            column("id", "raw_customers"),
            column("name", "raw_customers")
        ]
    );
}

#[test]
fn test_cte_select_wildcard_from_table_through_another_cte() {
    let output = test_sql(
        "WITH source AS (SELECT * FROM raw_customers),
              renamed AS (SELECT id AS customer_id FROM source)
         SELECT customer_id FROM renamed",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "customer_id".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("raw_customers")),
                name: "id".to_string()
            }]
        }]
    );
}

#[test]
fn test_cte_select_wildcard_from_cte() {
    let output = test_sql(
        "WITH base AS (SELECT a AS id FROM t1),
              source AS (SELECT * FROM base)
         SELECT id FROM source",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "id".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("t1")),
                name: "a".to_string()
            }]
        }]
    );
}

#[test]
fn test_derived_table_select_wildcard_from_table() {
    let output = test_sql("SELECT s.id, s.name FROM (SELECT * FROM raw_customers) s").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            column("id", "raw_customers"),
            column("name", "raw_customers")
        ]
    );
}

#[test]
fn test_cte_select_wildcard_from_join_keeps_cte_origin() {
    let output = test_sql(
        "WITH source AS (
            SELECT * FROM raw_customers c JOIN raw_orders o ON c.id = o.customer_id
         )
         SELECT id FROM source",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "source")]);
}

#[test]
fn test_cte_select_wildcard_with_modifier_keeps_cte_origin() {
    let output = test_sql_dialect(
        "WITH source AS (SELECT * EXCLUDE (secret) FROM raw_customers)
         SELECT id FROM source",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "source")]);
}
