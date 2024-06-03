// Copyright 2018-2024 contributors to the OpenLineage project
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

    pub fn get_table_from_alias(&self, alias: String) -> Option<&DbTableMeta> {
        self.table_aliases.iter().find_map(|entry| {
            if entry.0.qualified_name() == alias {
                Some(entry.1)
            } else {
                None
            }
        })
    }

    pub fn resolve_table<'a>(&'a self, name: &'a DbTableMeta) -> &'a DbTableMeta {
        let mut max_iter = 20; // does anyone need more than 20 aliases?
        let mut current = name;
        while let Some(next) = self.get_table_from_alias(current.qualified_name()) {
            current = next;
            max_iter -= 1;
            if max_iter <= 0 {
                return current;
            }
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
