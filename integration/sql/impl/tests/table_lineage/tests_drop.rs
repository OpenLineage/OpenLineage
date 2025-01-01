// Copyright 2018-2025 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::TableLineage;

#[test]
fn drop_table() {
    assert_eq!(
        test_sql("DROP TABLE IF EXISTS a.b, c.d")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["a.b", "c.d"])
        }
    );
}
