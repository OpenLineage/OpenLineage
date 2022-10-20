// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_nested_renaming() {
    let output = test_sql(
        "SELECT t1.a, t2.c as d
         FROM table1 t1
         INNER JOIN (SELECT t2.a, t2.b as c FROM table2 t2) t2
         ON t1.a = t2.a",
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
                    origin: Some(table("table1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "d".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table2")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}
