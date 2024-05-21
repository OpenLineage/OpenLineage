// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::TableLineage;

#[test]
fn delete_from() {
    assert_eq!(
        test_sql("DELETE FROM a.b WHERE x = 0",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["a.b"])
        }
    );
}

#[test]
fn delete_without_from_bigquery() {
    assert_eq!(
        test_sql_dialect("DELETE \"project.dataset.table\" WHERE 1", "bigquery")
            .unwrap()
            .table_lineage
            .out_tables
            .first()
            .unwrap()
            .qualified_name(),
        "project.dataset.table"
    )
}

#[test]
fn delete_from_using() {
    assert_eq!(
        test_sql(
            "DELETE FROM a.b AS t
                USING (
                    SELECT col
                    FROM b.c
                    WHERE col = 'x'
                ) AS duplicates
                WHERE a.b.col = duplicates.col",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["b.c"]),
            out_tables: tables(vec!["a.b"])
        }
    );
}
