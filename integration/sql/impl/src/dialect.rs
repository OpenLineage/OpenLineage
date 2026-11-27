// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use sqlparser::ast::Ident;
use sqlparser::dialect::{
    AnsiDialect, BigQueryDialect, DatabricksDialect, Dialect, DuckDbDialect, GenericDialect,
    HiveDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, RedshiftSqlDialect, SQLiteDialect,
    SnowflakeDialect,
};

pub trait CanonicalDialect: Dialect {
    fn canonical_name<'b>(&self, name: &'b str) -> Option<&'b str>;
    fn as_base(&self) -> &dyn Dialect;
}

impl<T: Dialect> CanonicalDialect for T {
    fn canonical_name<'b>(&self, name: &'b str) -> Option<&'b str> {
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
        "databricks" => &DatabricksDialect,
        "duckdb" => &DuckDbDialect {},
        "snowflake" => &SnowflakeDialect,
        "postgres" => &PostgreSqlDialect {},
        "postgresql" => &PostgreSqlDialect {},
        "redshift" => &RedshiftSqlDialect {},
        "hive" => &HiveDialect {},
        "mysql" => &MySqlDialect {},
        "mssql" => &MsSqlDialect {},
        "sqlserver" => &MsSqlDialect {},
        "sqlite" => &SQLiteDialect {},
        "ansi" => &AnsiDialect {},
        "generic" => &GenericDialect,
        _ => &GenericDialect,
    }
}

pub fn get_generic_dialect(name: Option<String>) -> &'static dyn CanonicalDialect {
    if let Some(d) = name {
        get_dialect(d.as_str())
    } else {
        &GenericDialect
    }
}

/// The name an identifier denotes under the active dialect: two identifiers
/// produce the same key if and only if the dialect treats them as the same
/// relation name.
///
/// - MySQL compares identifiers verbatim (as it does table aliases on Unix
///   under its default configuration).
/// - BigQuery, Databricks, Hive, DuckDB, SQL Server, SQLite, and Redshift
///   (whose default `enable_case_sensitive_identifier` is false) fold all
///   identifiers to ASCII lower case regardless of quoting.
/// - PostgreSQL folds unquoted identifiers to ASCII lower case and preserves
///   quoted ones verbatim.
/// - Snowflake, ANSI, and the generic fallback fold unquoted identifiers to
///   ASCII upper case and preserve quoted ones verbatim.
pub(crate) fn fold_ident(value: &str, quoted: bool, dialect: &dyn Dialect) -> String {
    if dialect.is::<MySqlDialect>() {
        value.to_string()
    } else if dialect.is::<BigQueryDialect>()
        || dialect.is::<DatabricksDialect>()
        || dialect.is::<HiveDialect>()
        || dialect.is::<DuckDbDialect>()
        || dialect.is::<MsSqlDialect>()
        || dialect.is::<RedshiftSqlDialect>()
        || dialect.is::<SQLiteDialect>()
    {
        value.to_ascii_lowercase()
    } else if quoted {
        value.to_string()
    } else if dialect.is::<PostgreSqlDialect>() {
        value.to_ascii_lowercase()
    } else {
        value.to_ascii_uppercase()
    }
}

pub(crate) fn ident_key(ident: &Ident, dialect: &dyn Dialect) -> String {
    fold_ident(&ident.value, ident.quote_style.is_some(), dialect)
}

/// Whether a CTE in this dialect may refer to itself without `WITH RECURSIVE`.
pub(crate) fn allows_implicit_recursive_cte(dialect: &dyn Dialect) -> bool {
    dialect.is::<SnowflakeDialect>()
        || dialect.is::<MsSqlDialect>()
        || dialect.is::<SQLiteDialect>()
}
