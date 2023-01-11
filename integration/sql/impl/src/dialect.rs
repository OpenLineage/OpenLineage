// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::bigquery::BigQueryDialect;
use sqlparser::dialect::{
    AnsiDialect, Dialect, GenericDialect, HiveDialect, MsSqlDialect, MySqlDialect,
    PostgreSqlDialect, RedshiftSqlDialect, SQLiteDialect, SnowflakeDialect,
};

pub trait CanonicalDialect: Dialect {
    fn canonical_name<'a, 'b>(&'a self, name: &'b str) -> Option<&'b str>;
    fn as_base(&self) -> &dyn Dialect;
}

impl<T: Dialect> CanonicalDialect for T {
    fn canonical_name<'a, 'b>(&'a self, name: &'b str) -> Option<&'b str> {
        name.chars().next().map(|x| {
            if self.is_delimited_identifier_start(x) {
                let mut chars = name.chars();
                chars.next();
                chars.next_back();
                chars.as_str()
            } else {
                name
            }
        })
    }
    fn as_base(&self) -> &dyn Dialect {
        self
    }
}

pub fn get_dialect(name: &str) -> &'static dyn CanonicalDialect {
    match name {
        "bigquery" => &BigQueryDialect,
        "snowflake" => &SnowflakeDialect,
        "postgres" => &PostgreSqlDialect {},
        "postgresql" => &PostgreSqlDialect {},
        "redshift" => &RedshiftSqlDialect {},
        "hive" => &HiveDialect {},
        "mysql" => &MySqlDialect {},
        "mssql" => &MsSqlDialect {},
        "sqlite" => &SQLiteDialect {},
        "ansi" => &AnsiDialect {},
        "generic" => &GenericDialect,
        _ => &GenericDialect,
    }
}

pub fn get_generic_dialect(name: Option<&str>) -> &'static dyn CanonicalDialect {
    if let Some(d) = name {
        get_dialect(d)
    } else {
        &GenericDialect
    }
}
