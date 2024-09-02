// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::CanonicalDialect;

use sqlparser::ast::Ident;
use sqlparser::dialect::SnowflakeDialect;

pub mod ident_wrapper {
    pub use sqlparser::ast::Ident as IdentWrapper;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExtractionError {
    pub index: usize,
    pub message: String,
    pub origin_statement: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlMeta {
    pub table_lineage: TableLineage,
    pub column_lineage: Vec<ColumnLineage>,
    pub errors: Vec<ExtractionError>,
}

impl SqlMeta {
    pub fn new(
        mut in_tables: Vec<DbTableMeta>,
        mut out_tables: Vec<DbTableMeta>,
        mut column_lineage: Vec<ColumnLineage>,
        errors: Vec<ExtractionError>,
    ) -> Self {
        in_tables.sort();
        out_tables.sort();
        column_lineage.sort_by(|l1, l2| l1.descendant.cmp(&l2.descendant));
        column_lineage.iter_mut().for_each(|x| x.lineage.sort());
        SqlMeta {
            table_lineage: TableLineage {
                in_tables,
                out_tables,
            },
            column_lineage,
            errors,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct ColumnLineage {
    pub descendant: ColumnMeta,
    pub lineage: Vec<ColumnMeta>,
}

impl ColumnLineage {
    pub fn new(descendant: ColumnMeta) -> Self {
        ColumnLineage {
            descendant,
            lineage: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ColumnMeta {
    pub origin: Option<DbTableMeta>,
    pub name: String,
}

impl ColumnMeta {
    pub fn new(name: String, origin: Option<DbTableMeta>) -> Self {
        ColumnMeta { name, origin }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableLineage {
    pub in_tables: Vec<DbTableMeta>,
    pub out_tables: Vec<DbTableMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct DbTableMeta {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub name: String,
    pub quote_style: Option<QuoteStyle>,
    pub provided_namespace: bool,
    pub provided_field_schema: bool,
    // ..columns
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct QuoteStyle {
    pub database: Option<char>,
    pub schema: Option<char>,
    pub name: Option<char>,
}

impl DbTableMeta {
    pub fn new(
        identifiers: Vec<Ident>,
        dialect: &dyn CanonicalDialect,
        default_schema: Option<String>,
        default_database: Option<String>,
    ) -> Self {
        DbTableMeta::new_with_namespace_and_schema(
            identifiers,
            dialect,
            default_schema,
            default_database,
            true,
            true,
            true,
        )
    }

    pub fn new_with_namespace_and_schema(
        identifiers: Vec<Ident>,
        dialect: &dyn CanonicalDialect,
        default_schema: Option<String>,
        default_database: Option<String>,
        provided_namespace: bool,
        provided_field_schema: bool,
        with_split_name: bool,
    ) -> Self {
        if !with_split_name {
            let name = identifiers
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<String>>()
                .join(".");
            // for example: snowflake external location with no namespace nor name split
            return DbTableMeta {
                database: None,
                schema: None,
                name,
                provided_namespace,
                provided_field_schema,
                quote_style: Some(QuoteStyle {
                    database: None,
                    schema: None,
                    name: identifiers.first().unwrap().quote_style,
                }),
            };
        }
        let reversed: Vec<Ident> = identifiers
            .iter()
            .rev()
            .map(|ident| {
                let name = ident.value.as_str();
                let canonical_name = dialect.canonical_name(name).unwrap_or(name);
                if let Some(quote_style) = ident.quote_style {
                    Ident::with_quote(quote_style, canonical_name)
                } else {
                    Ident::new(canonical_name)
                }
            })
            .collect();
        let table_name: &str = reversed.first().unwrap().value.as_str();
        DbTableMeta {
            database: reversed
                .get(2)
                .map(|ident| ident.value.as_str().to_string())
                .or(default_database),
            schema: reversed
                .get(1)
                .map(|ident| ident.value.as_str().to_string())
                .or(default_schema),
            name: table_name.to_string(),
            provided_namespace: false,
            provided_field_schema: false,
            quote_style: Some(QuoteStyle {
                database: reversed.get(2).and_then(|ident| ident.quote_style),
                schema: reversed.get(1).and_then(|ident| ident.quote_style),
                name: reversed.first().and_then(|ident| ident.quote_style),
            }),
        }
    }

    pub fn qualified_name(&self) -> String {
        format!(
            "{}{}{}",
            self.database
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or_default(),
            self.schema
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or_default(),
            self.name
        )
    }

    pub fn new_default_dialect(name: String) -> Self {
        let mut quote = None;
        let split = name
            .split('.')
            .map(|x| {
                let length = x.len();
                let first = x.chars().next().unwrap_or_default();
                let middle_part: String = if length <= 1 {
                    x.to_string()
                } else {
                    x[1..length - 1].to_string()
                };

                if first == '\'' || first == '"' || first == '`' || first == '[' {
                    quote = Some(first);
                }

                match quote {
                    Some(quote) => Ident::with_quote(quote, middle_part),
                    None => Ident::new(x.to_string()),
                }
            })
            .collect::<Vec<_>>();
        Self::new(split, &SnowflakeDialect, None, None)
    }

    pub fn new_default_dialect_with_namespace_and_schema(
        name: String,
        provided_namespace: bool,
        provided_field_schema: bool,
    ) -> Self {
        Self::new_with_namespace_and_schema(
            vec![Ident::new(name)],
            &SnowflakeDialect,
            None,
            None,
            provided_namespace,
            provided_field_schema,
            false,
        )
    }
}
