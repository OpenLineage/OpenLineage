// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::ops::Deref;

use crate::dialect::CanonicalDialect;
use crate::lineage::*;

use sqlparser::dialect::SnowflakeDialect;

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
    // Set of aliases we discovered in this query. We don't want to return alias as input and
    // output, because they have no sense in outside of query they were defined
    aliases: HashSet<DbTableMeta>,
    // Tables used as input to this query. "Input" is defined liberally, query does not have
    // to read data to be treated as input - it's sufficient that it's referenced in a query somehow
    inputs: HashSet<DbTableMeta>,
    // Tables used as output to this query. Same as input, they have to be referenced - data does
    // not have to be actually written as a result of execution.
    outputs: HashSet<DbTableMeta>,
    columns: HashMap<String, ColumnLineage>,
    // Some databases allow to specify default schema. When schema for table is not referenced,
    // we're using this default as it.
    default_schema: Option<String>,
    // Dialect used in this statements.
    dialect: &'a dyn CanonicalDialect,
}

impl<'a> Context<'a> {
    pub fn default() -> Context<'a> {
        Context {
            aliases: HashSet::new(),
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            columns: HashMap::new(),
            default_schema: None,
            dialect: &SnowflakeDialect,
        }
    }

    pub fn new(dialect: &dyn CanonicalDialect, default_schema: Option<String>) -> Context {
        Context {
            aliases: HashSet::new(),
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            columns: HashMap::new(),
            default_schema,
            dialect,
        }
    }

    pub fn add_alias(&mut self, alias: String) {
        let name = DbTableMeta::new(alias, self.dialect.deref(), self.default_schema.clone());
        self.aliases.insert(name);
    }

    pub fn add_input(&mut self, table: String) {
        let name = DbTableMeta::new(table, self.dialect.deref(), self.default_schema.clone());
        if !self.aliases.contains(&name) {
            self.inputs.insert(name);
        }
    }

    pub fn add_output(&mut self, output: String) {
        let name = DbTableMeta::new(output, self.dialect.deref(), self.default_schema.clone());
        if !self.aliases.contains(&name) {
            self.outputs.insert(name);
        }
    }

    pub fn inputs(&self) -> &HashSet<DbTableMeta> {
        &self.inputs
    }

    pub fn mut_inputs(&mut self) -> &mut HashSet<DbTableMeta> {
        &mut self.inputs
    }

    pub fn outputs(&self) -> &HashSet<DbTableMeta> {
        &self.outputs
    }

    pub fn mut_outputs(&mut self) -> &mut HashSet<DbTableMeta> {
        &mut self.outputs
    }
}
