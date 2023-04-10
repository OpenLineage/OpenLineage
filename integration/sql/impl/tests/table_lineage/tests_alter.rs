// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::{parse_sql, TableLineage};
use sqlparser::dialect::SnowflakeDialect;

#[test]
fn alter_contains_modified_table_as_output() {
    assert_eq!(
        test_sql("ALTER TABLE tab1 ADD COLUMN b int")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec![]),
            out_tables: tables(vec!["tab1"]),
        }
    )
}

#[test]
fn alter_table_rename() {
    assert_eq!(
        test_sql("ALTER TABLE tab1 RENAME TO tab2")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["tab1"]),
            out_tables: tables(vec!["tab2"]),
        }
    )
}

#[test]
fn alter_snowflake_swap_with() {
    assert_eq!(
        parse_sql("ALTER TABLE tab1 SWAP WITH tab2", &SnowflakeDialect, None)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["tab1", "tab2"]),
            out_tables: tables(vec!["tab1", "tab2"]),
        }
    )
}
