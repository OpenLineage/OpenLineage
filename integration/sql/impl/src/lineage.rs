// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::CanonicalDialect;

use sqlparser::dialect::SnowflakeDialect;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlMeta {
    pub table_lineage: TableLineage,
    pub column_lineage: Vec<ColumnLineage>,
}

impl SqlMeta {
    pub fn new(
        mut in_tables: Vec<DbTableMeta>,
        mut out_tables: Vec<DbTableMeta>,
        mut column_lineage: Vec<ColumnLineage>,
    ) -> Self {
        in_tables.sort();
        out_tables.sort();
        column_lineage.sort_by(|l1, l2| l1.descendant.cmp(&l2.descendant));
        SqlMeta {
            table_lineage: TableLineage {
                in_tables,
                out_tables,
            },
            column_lineage,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    pub fn new_default_dialect(name: String) -> Self {
        Self::new(name, &SnowflakeDialect, None)
    }
}
