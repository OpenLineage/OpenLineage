// Copyright 2018-2025 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use anyhow::Result;
use openlineage_sql::{get_dialect, parse_multiple_statements, parse_sql, DbTableMeta, SqlMeta};

pub fn test_sql(sql: &str) -> Result<SqlMeta> {
    test_sql_dialect(sql, "postgres")
}

pub fn test_multiple_sql(sqls: Vec<&str>) -> Result<SqlMeta> {
    test_multiple_sql_dialect(sqls, "postgres")
}

pub fn test_multiple_sql_dialect(sqls: Vec<&str>, dialect: &str) -> Result<SqlMeta> {
    parse_multiple_statements(
        sqls.iter().map(|x: &&str| x.to_string()).collect(),
        get_dialect(dialect),
        None,
    )
}

pub fn test_sql_dialect(sql: &str, dialect: &str) -> Result<SqlMeta> {
    parse_sql(sql, get_dialect(dialect), None)
}

pub fn table(name: &str) -> DbTableMeta {
    DbTableMeta::new_default_dialect(name.to_string())
}

pub fn tables(names: Vec<&str>) -> Vec<DbTableMeta> {
    names.into_iter().map(table).collect()
}
