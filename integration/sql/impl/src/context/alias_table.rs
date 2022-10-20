// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::lineage::{ColumnMeta, DbTableMeta};

use std::collections::HashMap;

#[derive(Debug)]
pub struct AliasTable {
    column_aliases: HashMap<ColumnMeta, ColumnMeta>,
    table_aliases: HashMap<DbTableMeta, DbTableMeta>,
}

impl AliasTable {
    pub fn new() -> Self {
        AliasTable { column_aliases: HashMap::new(), table_aliases: HashMap::new() }
    }

    pub fn add_column_alias(&mut self, col: ColumnMeta, alias: ColumnMeta) {
        self.column_aliases.insert(alias, col);
    }

    pub fn add_table_alias(&mut self, table: DbTableMeta, alias: DbTableMeta) {
        self.table_aliases.insert(alias, table);
    }

    pub fn resolve_column<'a>(&'a self, name: &'a ColumnMeta) -> &'a ColumnMeta {
        let mut current = name;
        while let Some(next) = self.column_aliases.get(current) {
            current = next;
        }
        current
    }

    pub fn resolve_table<'a>(&'a self, name: &'a DbTableMeta) -> &'a DbTableMeta {
        let mut current = name;
        while let Some(next) = self.table_aliases.get(current) {
            current = next;
        }
        current
    }

    pub fn is_column_alias(&self, name: &ColumnMeta) -> bool {
        self.resolve_column(name) != name
    }

    pub fn is_table_alias(&self, name: &DbTableMeta) -> bool {
        self.resolve_table(name) != name
    }
}