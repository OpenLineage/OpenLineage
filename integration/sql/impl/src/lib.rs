// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

mod bigquery;
mod context;
mod dialect;
mod lineage;
mod visitor;

use std::collections::HashSet;

pub use bigquery::BigQueryDialect;
use context::Context;
pub use dialect::*;
pub use lineage::*;
use visitor::Visit;

use anyhow::{anyhow, Result};
use sqlparser::parser::Parser;

pub fn parse_multiple_statements(
    sql: Vec<&str>,
    dialect: &dyn CanonicalDialect,
    default_schema: Option<String>,
) -> Result<SqlMeta> {
    let mut inputs: HashSet<DbTableMeta> = HashSet::new();
    let mut outputs: HashSet<DbTableMeta> = HashSet::new();
    let mut column_lineage: Vec<ColumnLineage> = vec![];

    for statement in sql {
        let ast = Parser::parse_sql(dialect.as_base(), statement)?;

        if ast.is_empty() {
            return Err(anyhow!("Empty statement list"));
        }

        for stmt in ast {
            let mut context = Context::new(dialect.clone(), default_schema.clone());
            stmt.visit(&mut context)?;
            inputs.extend(context.inputs.drain());
            outputs.extend(context.outputs.drain());
            column_lineage.extend(context.columns.drain().map(|(descendant, lineage)| {
                ColumnLineage {
                    descendant: ColumnMeta::new(descendant, None),
                    lineage,
                }
            }));
        }
    }
    Ok(SqlMeta::new(
        inputs.into_iter().collect(),
        outputs.into_iter().collect(),
        column_lineage,
    ))
}

pub fn parse_sql(
    sql: &str,
    dialect: &dyn CanonicalDialect,
    default_schema: Option<String>,
) -> Result<SqlMeta> {
    parse_multiple_statements(vec![sql], dialect, default_schema)
}

#[cfg(test)]
mod tests {
    use crate::DbTableMeta;

    #[test]
    fn compare_db_meta() {
        assert_ne!(
            DbTableMeta {
                database: None,
                schema: None,
                name: "discount".to_string()
            },
            DbTableMeta {
                database: None,
                schema: Some("public".to_string()),
                name: "discount".to_string()
            }
        );
    }
}
