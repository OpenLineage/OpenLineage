// Copyright 2018-2023 contributors to the OpenLineage project
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
use sqlparser::parser::ParserError::ParserError;

pub fn parse_multiple_statements(
    sql: Vec<&str>,
    dialect: &dyn CanonicalDialect,
    default_schema: Option<String>,
) -> Result<SqlMeta> {
    let mut inputs: HashSet<DbTableMeta> = HashSet::new();
    let mut outputs: HashSet<DbTableMeta> = HashSet::new();
    let mut column_lineage: Vec<ColumnLineage> = vec![];
    let mut errors: Vec<ExtractionError> = vec![];

    for (index, statement) in sql.iter().enumerate() {
        let ast = Parser::parse_sql(dialect.as_base(), statement);

        if let Err(ParserError(s)) = ast {
            errors.push(ExtractionError {
                index,
                message: s.clone(),
                origin_statement: statement.to_string(),
            });
            continue;
        }
        let ast = ast.unwrap();

        if ast.is_empty() {
            return Err(anyhow!("Empty statement list"));
        }

        for stmt in ast {
            let mut context = Context::new(dialect, default_schema.clone());
            stmt.visit(&mut context)?;
            column_lineage.extend(context.mut_columns().drain().map(|(descendant, lineage)| {
                ColumnLineage {
                    descendant,
                    lineage: Vec::from_iter(lineage),
                }
            }));
            inputs.extend(context.inputs);
            outputs.extend(context.outputs);
        }
    }
    Ok(SqlMeta::new(
        inputs.into_iter().collect(),
        outputs.into_iter().collect(),
        column_lineage,
        errors,
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
                name: "discount".to_string(),
                provided_namespace: false,
                provided_field_schema: false,
            },
            DbTableMeta {
                database: None,
                schema: Some("public".to_string()),
                name: "discount".to_string(),
                provided_namespace: false,
                provided_field_schema: false,
            }
        );
    }
}
