// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

mod alias_table;

use std::collections::{HashMap, HashSet};
use std::ops::Deref;

use crate::dialect::CanonicalDialect;
use crate::lineage::*;
use alias_table::AliasTable;

use sqlparser::dialect::SnowflakeDialect;

type ColumnAncestors = HashSet<ColumnMeta>;

#[derive(Debug)]
pub struct ContextFrame {
    // Indicates whether we are talking about columns in a context of some specific table.
    // For example, when we visit a select statement 'SELECT a, b FROM t', our table context
    // would be 't'
    table: Option<DbTableMeta>,
    // Specifies whether we are inside a column context and its name
    column: Option<ColumnMeta>,
    // The alias table is used to track back a chain of aliases back to the original
    // symbol. Only those symbols are meaningful outside of the processed query.
    aliases: AliasTable,
    pub column_ancestry: HashMap<ColumnMeta, ColumnAncestors>,
}

impl ContextFrame {
    fn new() -> Self {
        ContextFrame {
            table: None,
            column: None,
            aliases: AliasTable::new(),
            column_ancestry: HashMap::new(),
        }
    }
}

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
    // A stack of context frames allows for precise control over the scope of the information
    // within. For example, this prevents shadowed symbols from being forgotten.
    frames: Vec<ContextFrame>,
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
            frames: vec![ContextFrame::new()],
            default_schema: None,
            dialect: &SnowflakeDialect,
            column_id: 0,
        }
    }

    pub fn new(dialect: &dyn CanonicalDialect, default_schema: Option<String>) -> Context {
        Context {
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            frames: vec![ContextFrame::new()],
            default_schema,
            dialect,
            column_id: 0,
        }
    }

    // --- Table Lineage ---

    pub fn add_input(&mut self, table: String) {
        let name = DbTableMeta::new(table, self.dialect.deref(), self.default_schema.clone());
        if !self.is_table_alias(&name) {
            self.inputs.insert(name);
        }
    }

    pub fn add_output(&mut self, output: String) {
        let name = DbTableMeta::new(output, self.dialect.deref(), self.default_schema.clone());
        if !self.is_table_alias(&name) {
            self.outputs.insert(name);
        }
    }

    // --- Column Lineage ---

    pub fn add_column_ancestors(&mut self, column: ColumnMeta, mut ancestors: Vec<ColumnMeta>) {
        if self.frames.last().is_none() {
            return;
        }

        let frame = self.frames.last_mut().unwrap();

        for ancestor in &mut ancestors {
            if let Some(table) = &mut ancestor.origin {
                *table = frame.aliases.resolve_table(table).clone();
            }
        }

        let entry = frame.column_ancestry.entry(column);
        entry
            .and_modify(|x| x.extend(ancestors.drain(..)))
            .or_insert(HashSet::from_iter(ancestors));
    }

    // --- Context Manipulators ---

    pub fn push_frame(&mut self) {
        let mut frame = ContextFrame::new();
        if let Some(recent) = self.frames.last() {
            frame.table = recent.table.clone();
            frame.column = recent.column.clone();
        }
        self.frames.push(frame);
    }

    pub fn pop_frame(&mut self) -> Option<ContextFrame> {
        self.frames.pop()
    }

    pub fn add_table_alias(&mut self, table: DbTableMeta, alias: String) {
        if let Some(frame) = self.frames.last_mut() {
            let alias = DbTableMeta::new(alias, self.dialect.deref(), self.default_schema.clone());
            frame.aliases.add_table_alias(table, alias);
        }
    }

    pub fn set_table_context(&mut self, table: Option<DbTableMeta>) {
        if let Some(frame) = self.frames.last_mut() {
            frame.table = table;
        }
    }

    pub fn set_column_context(&mut self, column: Option<ColumnMeta>) {
        if let Some(frame) = self.frames.last_mut() {
            frame.column = column;
        }
    }

    pub fn set_unnamed_column_context(&mut self) {
        let name = self.next_unnamed_column();
        if let Some(frame) = self.frames.last_mut() {
            frame.column.replace(ColumnMeta::new(name, None));
        }
    }

    // --- Methods Transcending Frame Stack ---

    pub fn resolve_table(&'a self, name: &'a DbTableMeta) -> &'a DbTableMeta {
        let mut current = name;
        for frame in self.frames.iter().rev() {
            current = frame.aliases.resolve_table(current);
        }
        current
    }

    pub fn is_table_alias(&self, name: &DbTableMeta) -> bool {
        self.resolve_table(name) != name
    }

    // --- Synthesize Attributes From Frames ---
    // These methods are used to retrieve information
    // from the narrower scope and allow the visitor
    // to compute synthesized attributes.

    pub fn coalesce(&mut self, mut old: ContextFrame) {
        if self.frames.last().is_none() {
            return;
        }

        let frame = self.frames.last_mut().unwrap();
        let mut result: HashMap<ColumnMeta, ColumnAncestors> = HashMap::new();

        for (col, ancestors) in &frame.column_ancestry {
            let mut expanded = ColumnAncestors::new();
            for ancestor in ancestors {
                let prev = old.column_ancestry.get(ancestor);
                if let Some(list) = prev {
                    expanded.extend(list.iter().cloned());
                } else {
                    expanded.insert(ancestor.clone());
                }
            }
            result.insert(col.clone(), expanded);
        }

        frame.column_ancestry = result;
    }

    pub fn collect(&mut self, mut old: ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.column_ancestry.extend(old.column_ancestry.drain());
        }
    }

    pub fn collect_with_table(&mut self, mut old: ContextFrame, from_table: DbTableMeta) {
        if let Some(frame) = self.frames.last_mut() {
            let old_ancestry = old
                .column_ancestry
                .drain()
                .map(|(mut col, anc)| {
                    col.origin = col.origin.or_else(|| Some(from_table.clone()));
                    (col, anc)
                })
                .collect::<HashMap<ColumnMeta, ColumnAncestors>>();
            frame.column_ancestry.extend(old_ancestry);
        }
    }

    pub fn collect_aliases(&mut self, old: &ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            for (alias, t) in old.aliases.tables() {
                frame.aliases.add_table_alias(t.clone(), alias.clone());
            }
        }
    }

    // --- Accessors ---
    // For accessors returning a reference, we generally assume
    // that there is at least one frame on the frame stack
    // and we panic otherwise. This should always hold true,
    // as Context keeps a global frame by default.

    pub fn columns(&self) -> &HashMap<ColumnMeta, ColumnAncestors> {
        let frame = self.frames.last().unwrap();
        &frame.column_ancestry
    }

    pub fn mut_columns(&mut self) -> &mut HashMap<ColumnMeta, ColumnAncestors> {
        let frame = self.frames.last_mut().unwrap();
        &mut frame.column_ancestry
    }

    pub fn table_context(&self) -> &Option<DbTableMeta> {
        let frame = self.frames.last().unwrap();
        &frame.table
    }

    pub fn column_context(&self) -> &Option<ColumnMeta> {
        let frame = self.frames.last().unwrap();
        &frame.column
    }

    pub fn dialect(&self) -> &dyn CanonicalDialect {
        self.dialect
    }

    pub fn default_schema(&self) -> &Option<String> {
        &self.default_schema
    }

    // --- Utils ---

    fn next_unnamed_column(&mut self) -> String {
        let out = format!("_{}", self.column_id);
        self.column_id += 1;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lineage::*;

    #[test]
    fn test_coalesce_columns() {
        // We test the following ancestry structure
        // t2.x  t1.y    t1.b
        //   \   /         |
        //    \ /          |
        //     x           b    (selected from "table")
        //     |          /|
        //     |        /  |
        //     |      /    |
        //     |    /      |
        //     |  /        |
        //     |/          |
        //     c           d

        let mut context = Context::default();

        // frame #1
        context.push_frame();
        context.add_column_ancestors(
            ColumnMeta::new("c".to_string(), None),
            vec![
                ColumnMeta::new(
                    "a".to_string(),
                    Some(DbTableMeta::new_default_dialect("table".to_string())),
                ),
                ColumnMeta::new(
                    "b".to_string(),
                    Some(DbTableMeta::new_default_dialect("table".to_string())),
                ),
            ],
        );
        context.add_column_ancestors(
            ColumnMeta::new("d".to_string(), None),
            vec![ColumnMeta::new(
                "b".to_string(),
                Some(DbTableMeta::new_default_dialect("table".to_string())),
            )],
        );

        // frame #2
        context.push_frame();
        context.add_column_ancestors(
            ColumnMeta::new(
                "a".to_string(),
                Some(DbTableMeta::new_default_dialect("table".to_string())),
            ),
            vec![
                ColumnMeta::new(
                    "x".to_string(),
                    Some(DbTableMeta::new_default_dialect("t2".to_string())),
                ),
                ColumnMeta::new(
                    "y".to_string(),
                    Some(DbTableMeta::new_default_dialect("t1".to_string())),
                ),
            ],
        );
        context.add_column_ancestors(
            ColumnMeta::new(
                "b".to_string(),
                Some(DbTableMeta::new_default_dialect("table".to_string())),
            ),
            vec![ColumnMeta::new(
                "b".to_string(),
                Some(DbTableMeta::new_default_dialect("t1".to_string())),
            )],
        );

        // Coalesce ancestry from frame #2
        let old_frame = context.pop_frame().unwrap();
        context.coalesce(old_frame);

        let mut ancestry = Vec::from_iter(context.frames.last().unwrap().column_ancestry.iter());
        ancestry.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            ancestry,
            vec![
                (
                    &ColumnMeta::new("c".to_string(), None),
                    &HashSet::from([
                        ColumnMeta::new(
                            "x".to_string(),
                            Some(DbTableMeta::new_default_dialect("t2".to_string()))
                        ),
                        ColumnMeta::new(
                            "y".to_string(),
                            Some(DbTableMeta::new_default_dialect("t1".to_string()))
                        ),
                        ColumnMeta::new(
                            "b".to_string(),
                            Some(DbTableMeta::new_default_dialect("t1".to_string()))
                        ),
                    ])
                ),
                (
                    &ColumnMeta::new("d".to_string(), None),
                    &HashSet::from([ColumnMeta::new(
                        "b".to_string(),
                        Some(DbTableMeta::new_default_dialect("t1".to_string()))
                    ),])
                ),
            ]
        );
    }

    #[test]
    fn test_collect_with_table() {
        let mut context = Context::default();

        // frame #1
        context.push_frame();
        context.add_column_ancestors(
            ColumnMeta::new("c".to_string(), None),
            vec![ColumnMeta::new(
                "a".to_string(),
                Some(DbTableMeta::new_default_dialect("table".to_string())),
            )],
        );
        context.add_column_ancestors(
            ColumnMeta::new("d".to_string(), None),
            vec![ColumnMeta::new(
                "b".to_string(),
                Some(DbTableMeta::new_default_dialect("table".to_string())),
            )],
        );

        let frame = context.pop_frame().unwrap();
        context.collect_with_table(frame, DbTableMeta::new_default_dialect("alias".to_string()));

        let mut ancestry = Vec::from_iter(context.frames.last().unwrap().column_ancestry.iter());
        ancestry.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            ancestry,
            vec![
                (
                    &ColumnMeta::new(
                        "c".to_string(),
                        Some(DbTableMeta::new_default_dialect("alias".to_string()))
                    ),
                    &HashSet::from([ColumnMeta::new(
                        "a".to_string(),
                        Some(DbTableMeta::new_default_dialect("table".to_string()))
                    ),])
                ),
                (
                    &ColumnMeta::new(
                        "d".to_string(),
                        Some(DbTableMeta::new_default_dialect("alias".to_string()))
                    ),
                    &HashSet::from([ColumnMeta::new(
                        "b".to_string(),
                        Some(DbTableMeta::new_default_dialect("table".to_string()))
                    ),])
                ),
            ]
        );
    }
}
