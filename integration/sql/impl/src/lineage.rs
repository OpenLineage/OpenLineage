// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::CanonicalDialect;

use sqlparser::dialect::SnowflakeDialect;

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
        mut errors: Vec<ExtractionError>,
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
    // ..columns
}

impl DbTableMeta {
    pub fn new(
        name: String,
        dialect: &dyn CanonicalDialect,
        default_schema: Option<String>,
    ) -> Self {
        let mut split = name
            .split('.')
            .map(|x| dialect.canonical_name(x).unwrap_or(x))
            .collect::<Vec<&str>>();
        split.reverse();
        let table_name: &str = split.first().unwrap_or(&name.as_str());
        DbTableMeta {
            database: split.get(2).map(ToString::to_string),
            schema: split
                .get(1)
                .map(ToString::to_string)
                .or_else(|| default_schema),
            name: table_name.to_string(),
        }
    }

    pub fn qualified_name(&self) -> String {
        format!(
            "{}{}{}",
            self.database
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or_else(|| "".to_string()),
            self.schema
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or_else(|| "".to_string()),
            self.name
        )
    }

    pub fn new_default_dialect(name: String) -> Self {
        Self::new(name, &SnowflakeDialect, None)
    }
}
