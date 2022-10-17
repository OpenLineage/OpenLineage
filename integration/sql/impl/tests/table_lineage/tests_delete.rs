// Copyright 2018-2022 contributors to the OpenLineage project
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
            out_tables: table("a.b")
        }
    );
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
            in_tables: table("b.c"),
            out_tables: table("a.b")
        }
    );
}
