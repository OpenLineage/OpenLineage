// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::TableLineage;

#[test]
fn truncate_table() {
    assert_eq!(
        test_sql("TRUNCATE TABLE a.b",).unwrap().table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["a.b"])
        }
    );
}
