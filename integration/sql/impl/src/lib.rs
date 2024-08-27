// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

mod context;
mod dialect;
mod lineage;
mod visitor;

use context::Context;
pub use dialect::*;
pub use lineage::*;
use visitor::Visit;

use anyhow::{anyhow, Result};
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, RecursionLimitExceeded, TokenizerError};

pub fn parse_multiple_statements(
    sql: Vec<&str>,
    dialect: &dyn CanonicalDialect,
    default_schema: Option<String>,
) -> Result<SqlMeta> {
    let mut column_lineage: Vec<ColumnLineage> = vec![];
    let mut errors: Vec<ExtractionError> = vec![];
    let mut context = Context::new(dialect, default_schema.clone(), None);

    for (index, statement) in sql.iter().enumerate() {
        let ast = Parser::parse_sql(dialect.as_base(), statement);

        if let Err(x) = ast {
            let message = match x {
                TokenizerError(s) => s.clone(),
                ParserError(s) => s.clone(),
                RecursionLimitExceeded => {
                    { "Recursion limit exceeded when parsing SQL" }.to_string()
                }
            };
            errors.push(ExtractionError {
                index,
                message,
                origin_statement: statement.to_string(),
            });

            continue;
        }
        let ast = ast.unwrap();

        if ast.is_empty() {
            return Err(anyhow!("Empty statement list"));
        }

        for stmt in ast {
            stmt.visit(&mut context)?;
            column_lineage.extend(context.mut_columns().drain().map(|(descendant, lineage)| {
                ColumnLineage {
                    descendant,
                    lineage: Vec::from_iter(lineage),
                }
            }));
        }
    }
    Ok(SqlMeta::new(
        context.inputs.into_iter().collect(),
        context.outputs.into_iter().collect(),
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
    use crate::{DbTableMeta, QuoteStyle};

    #[test]
    fn compare_db_meta() {
        assert_ne!(
            DbTableMeta {
                database: None,
                schema: None,
                name: "discount".to_string(),
                provided_namespace: false,
                provided_field_schema: false,
                quote_style: Some(QuoteStyle {
                    database: None,
                    schema: None,
                    name: Some('"')
                })
            },
            DbTableMeta {
                database: None,
                schema: Some("public".to_string()),
                name: "discount".to_string(),
                provided_namespace: false,
                provided_field_schema: false,
                quote_style: Some(QuoteStyle {
                    database: None,
                    schema: None,
                    name: Some('"')
                })
            }
        );
    }
}
