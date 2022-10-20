// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

mod alias_table;

use std::collections::{HashMap, HashSet};
use std::ops::Deref;

use crate::dialect::CanonicalDialect;
use crate::lineage::*;
use alias_table::AliasTable;

use sqlparser::dialect::SnowflakeDialect;

type ColumnAncestors = HashSet<ColumnMeta>;

// Context struct serves as generic holder of an all information we currently have about
// SQL statements that we have parsed so far.
//
// TODO: properly handle "nested" and linked contexts.
// For example, right now when handling multiple statements we don't create contexts.
// This is because we want to interpret inputs and outputs of a multiple statements
// as a single list of inputs and outputs - since that's what's our parser contract.
// Caller like Airflow Extractor does not know how much queries were executed separated by commas
// since it's supposed to be opaque blob - so we want to keep input and output info.
// However, aliases are lost when going from statement to statement, so we need to handle that.
#[derive(Debug)]
pub struct Context<'a> {
    // Tables used as input to this query. "Input" is defined liberally, query does not have
    // to read data to be treated as input - it's sufficient that it's referenced in a query somehow
    pub inputs: HashSet<DbTableMeta>,
    // Tables used as output to this query. Same as input, they have to be referenced - data does
    // not have to be actually written as a result of execution.
    pub outputs: HashSet<DbTableMeta>,
    // Map of column lineages
    pub columns: HashMap<String, ColumnAncestors>,
    // The alias table is used to track back a chain of aliases back to the original
    // symbol. Only those symbols are meaningful outside of the processed query.
    aliases: AliasTable,
    // Indicates whether we are talking about columns in a context of some specific table.
    // For example, when we visit a select statement 'SELECT a, b FROM t', our table context
    // would be 't'
    table_context: Option<DbTableMeta>,
    // Specifies whether we are inside a column context and its name
    column_context: Option<ColumnMeta>,
    // Some databases allow to specify default schema. When schema for table is not referenced,
    // we're using this default as it.
    default_schema: Option<String>,
    // Dialect used in this statements.
    dialect: &'a dyn CanonicalDialect,
    // Used to generate unique names for unaliased columns created from compound expressions
    column_id: u32,
}

impl<'a> Context<'a> {
    pub fn default() -> Context<'a> {
        Context {
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            columns: HashMap::new(),
            aliases: AliasTable::new(),
            table_context: None,
            column_context: None,
            default_schema: None,
            dialect: &SnowflakeDialect,
            column_id: 0,
        }
    }

    pub fn new(dialect: &dyn CanonicalDialect, default_schema: Option<String>) -> Context {
        Context {
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            columns: HashMap::new(),
            aliases: AliasTable::new(),
            table_context: None,
            column_context: None,
            default_schema,
            dialect,
            column_id: 0,
        }
    }

    // --- Table Lineage ---

    pub fn add_input(&mut self, table: String) {
        let name = DbTableMeta::new(table, self.dialect.deref(), self.default_schema.clone());
        if !self.aliases.is_table_alias(&name) {
            self.inputs.insert(name);
        }
    }

    pub fn add_output(&mut self, output: String) {
        let name = DbTableMeta::new(output, self.dialect.deref(), self.default_schema.clone());
        if !self.aliases.is_table_alias(&name) {
            self.outputs.insert(name);
        }
    }

    // --- Column Lineage ---

    pub fn add_column_ancestors(&mut self, column: String, mut ancestors: Vec<ColumnMeta>) {
        for ancestor in &mut ancestors {
            if let Some(table) = &mut ancestor.origin {
                *table = self.aliases.resolve_table(table).clone();
            }
        }

        let entry = self.columns.entry(column);
        entry
            .and_modify(|x| x.extend(ancestors.drain(..)))
            .or_insert(HashSet::from_iter(ancestors));
    }

    // --- Context Manipulators ---

    pub fn add_table_alias(&mut self, table: DbTableMeta, alias: String) {
        let alias = DbTableMeta::new(alias, self.dialect.deref(), self.default_schema.clone());
        self.aliases.add_table_alias(table, alias);
    }

    pub fn set_table_context(&mut self, table: Option<DbTableMeta>) {
        self.table_context = table;
    }

    pub fn set_column_context(&mut self, column: Option<ColumnMeta>) {
        self.column_context = column;
    }

    pub fn set_unnamed_column_context(&mut self) {
        self.column_context = Some(ColumnMeta::new(self.next_unnamed_column(), None));
    }

    // --- Accessors ---
    pub fn table_context(&self) -> &Option<DbTableMeta> {
        &self.table_context
    }

    pub fn column_context(&self) -> &Option<ColumnMeta> {
        &self.column_context
    }

    pub fn dialect(&self) -> &dyn CanonicalDialect {
        self.dialect
    }

    pub fn default_schema(&self) -> &Option<String> {
        &self.default_schema
    }

    // --- Utils ---
    fn next_unnamed_column(&self) -> String {
        format!("_{}", self.column_id)
    }
}
