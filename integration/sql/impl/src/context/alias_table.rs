// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::lineage::DbTableMeta;

use std::collections::HashMap;

#[derive(Debug)]
pub struct AliasTable {
    table_aliases: HashMap<DbTableMeta, DbTableMeta>,
}

impl AliasTable {
    pub fn new() -> Self {
        AliasTable {
            table_aliases: HashMap::new(),
        }
    }

    pub fn add_table_alias(&mut self, table: DbTableMeta, alias: DbTableMeta) {
        self.table_aliases.insert(alias, table);
    }

    pub fn resolve_table<'a>(&'a self, name: &'a DbTableMeta) -> &'a DbTableMeta {
        let mut current = name;
        while let Some(next) = self.table_aliases.get(current) {
            current = next;
        }
        current
    }

    #[allow(dead_code)]
    pub fn is_table_alias(&self, name: &DbTableMeta) -> bool {
        self.resolve_table(name) != name
    }

    pub fn tables(&self) -> &HashMap<DbTableMeta, DbTableMeta> {
        &self.table_aliases
    }
}
