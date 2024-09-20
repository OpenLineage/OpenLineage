// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

mod alias_table;

use std::collections::{HashMap, HashSet};

use crate::dialect::CanonicalDialect;
use crate::lineage::*;
use alias_table::AliasTable;
use sqlparser::ast::Ident;
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
    pub dependencies: HashSet<DbTableMeta>,
    pub cte_dependencies: HashMap<String, CteDependency>,
    pub is_main_body: bool,
}

#[derive(Debug, Clone)]
pub struct CteDependency {
    pub is_used: bool,
    pub deps: HashSet<DbTableMeta>,
}

impl ContextFrame {
    fn new() -> Self {
        ContextFrame {
            table: None,
            column: None,
            aliases: AliasTable::new(),
            column_ancestry: HashMap::new(),
            dependencies: HashSet::new(),
            cte_dependencies: HashMap::new(),
            is_main_body: true,
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
    // Some databases allow to specify default database. When database for table is not referenced,
    // we're using this default as it.
    default_database: Option<String>,
    // Dialect used in these statements.
    dialect: &'a dyn CanonicalDialect,
    // Used to generate unique names for unaliased columns created from compound expressions
    column_id: u32,
}

impl<'a> Context<'a> {
    #![allow(dead_code)]
    pub fn default() -> Context<'a> {
        Context {
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            frames: vec![ContextFrame::new()],
            default_schema: None,
            default_database: None,
            dialect: &SnowflakeDialect,
            column_id: 0,
        }
    }

    pub fn new(
        dialect: &dyn CanonicalDialect,
        default_schema: Option<String>,
        default_database: Option<String>,
    ) -> Context {
        Context {
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            frames: vec![ContextFrame::new()],
            default_schema,
            default_database,
            dialect,
            column_id: 0,
        }
    }

    // --- Table Lineage ---

    pub fn add_input(&mut self, table: Vec<Ident>) {
        let name = DbTableMeta::new(
            table,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        );
        if !self.is_table_alias(&name) {
            self.inputs.insert(name);
        }
    }

    pub fn add_non_table_input(
        &mut self,
        table: Vec<Ident>,
        provided_namespace: bool,
        provided_field_schema: bool,
    ) {
        let name = DbTableMeta::new_with_namespace_and_schema(
            table,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
            provided_namespace,
            provided_field_schema,
            false,
        );
        if !self.is_table_alias(&name) {
            self.inputs.insert(name);
        }
    }

    pub fn add_output(&mut self, output: Vec<Ident>) {
        let name = DbTableMeta::new(
            output,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        );
        if !self.is_table_alias(&name) {
            self.outputs.insert(name);
        }
    }

    pub fn add_non_table_output(
        &mut self,
        output: Vec<Ident>,
        provided_namespace: bool,
        provided_field_schema: bool,
    ) {
        let name = DbTableMeta::new_with_namespace_and_schema(
            output,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
            provided_namespace,
            provided_field_schema,
            false,
        );
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
            frame.table.clone_from(&recent.table);
            frame.column.clone_from(&recent.column);
            frame.is_main_body.clone_from(&recent.is_main_body);
        }
        self.frames.push(frame);
    }

    pub fn is_main(&mut self) -> bool {
        if let Some(frame) = self.frames.last_mut() {
            return frame.is_main_body;
        }

        false
    }

    pub fn set_frame_to_main_body(&mut self) {
        self.frames.last_mut().unwrap().is_main_body = true;
    }

    pub fn unset_frame_to_main_body(&mut self) {
        self.frames.last_mut().unwrap().is_main_body = false;
    }

    pub fn pop_frame(&mut self) -> Option<ContextFrame> {
        self.frames.pop()
    }

    pub fn add_table_alias(&mut self, table: DbTableMeta, alias: Vec<Ident>) {
        if let Some(frame) = self.frames.last_mut() {
            let alias = DbTableMeta::new(
                alias,
                self.dialect,
                self.default_schema.clone(),
                self.default_database.clone(),
            );
            frame.aliases.add_table_alias(table, alias);
        }
    }

    pub fn add_table_dependency(&mut self, table: Vec<Ident>) {
        let name = DbTableMeta::new(
            table,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        );

        if let Some(frame) = self.frames.last_mut() {
            frame.dependencies.extend(vec![name]);
        }
    }

    pub fn has_alias(&self, table: String) -> bool {
        for frame in self.frames.iter().rev() {
            if frame.cte_dependencies.contains_key(&table) {
                return true;
            }
        }

        false
    }

    pub fn mark_table_as_used(&mut self, table: Vec<Ident>) {
        let name = DbTableMeta::new(
            table,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        );

        for frame in self.frames.iter_mut().rev() {
            if frame.cte_dependencies.contains_key(&name.qualified_name()) {
                if let Some(cte) = frame.cte_dependencies.get_mut(&name.qualified_name()) {
                    cte.is_used = true;
                }

                return;
            }
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

    pub fn set_default_database(&mut self, database: Option<String>) {
        if let Some(db) = database {
            self.default_database = Some(db);
        }
    }

    pub fn set_default_schema(&mut self, schema: Option<String>) {
        if let Some(sch) = schema {
            self.default_schema = Some(sch);
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

    pub fn coalesce(&mut self, old: ContextFrame) {
        if self.frames.last().is_none() {
            return;
        }

        let frame = self.frames.last_mut().unwrap();
        let mut result: HashMap<ColumnMeta, ColumnAncestors> = HashMap::new();

        for (col, ancestors) in &frame.column_ancestry {
            let mut expanded = ColumnAncestors::new();
            for ancestor in ancestors {
                // here we need also look on other if they are connected
                let traversed = Context::expand_ancestors(ancestor.clone(), &old);

                if !traversed.is_empty() {
                    expanded.extend(traversed.iter().cloned());
                } else {
                    expanded.insert(ancestor.clone());
                }
            }
            result.insert(col.clone(), expanded);
        }

        frame.column_ancestry = result;
        frame.dependencies.extend(old.dependencies);
        frame.is_main_body = old.is_main_body;
    }

    fn expand_ancestors(ancestor: ColumnMeta, old: &ContextFrame) -> Vec<ColumnMeta> {
        let mut result = Vec::new();
        let mut stack = Vec::new();

        stack.push(ancestor.clone());

        while let Some(current) = stack.pop() {
            let column_ancestors = old.column_ancestry.get(&current);
            if column_ancestors.is_none() {
                result.push(current.clone());
                continue;
            }

            if let Some(ancestors) = column_ancestors {
                for ancestor in ancestors {
                    stack.push(ancestor.clone());
                }
            }
        }

        result
    }

    pub fn collect(&mut self, mut old: ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.column_ancestry.extend(old.column_ancestry.drain());
            frame.dependencies.extend(old.dependencies);
            frame.is_main_body = old.is_main_body;
        }
    }

    pub fn collect_inputs(
        &mut self,
        cte_deps: HashMap<String, CteDependency>,
        deps: HashSet<DbTableMeta>,
    ) {
        if cte_deps.is_empty() {
            for table in deps {
                self.inputs.insert(table.clone());
            }
            return;
        }

        for (_key, value) in cte_deps {
            if !value.is_used {
                continue;
            }

            for table in value.deps {
                self.inputs.insert(table.clone());
            }
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

            let removed_circular_deps =
                Context::remove_circular_deps(&old_ancestry, frame, from_table);

            if !removed_circular_deps.is_empty() {
                frame.column_ancestry.extend(old_ancestry);
            } else {
                frame.column_ancestry.extend(removed_circular_deps);
            }

            frame.dependencies.extend(old.dependencies);
            frame.cte_dependencies.extend(old.cte_dependencies);
            frame.is_main_body = old.is_main_body;
        }
    }

    fn remove_circular_deps(
        old_ancestry: &HashMap<ColumnMeta, ColumnAncestors>,
        frame: &ContextFrame,
        from_table: DbTableMeta,
    ) -> HashMap<ColumnMeta, ColumnAncestors> {
        let mut removed_circular_deps: HashMap<ColumnMeta, ColumnAncestors> = HashMap::new();

        for (key_column_meta, column_ancestors) in old_ancestry.iter() {
            for column_meta in column_ancestors {
                if let Some(origin) = column_meta.origin.clone() {
                    if frame.aliases.is_table_alias(&origin) && origin == from_table {
                        continue;
                    }
                }

                removed_circular_deps
                    .entry(key_column_meta.clone())
                    .or_default()
                    .insert(column_meta.clone());
            }
        }

        removed_circular_deps
    }

    pub fn collect_lower_nested_dependencies(&mut self, cte_name: String) {
        if let Some(frame) = self.frames.last_mut() {
            let deps_in_one_level_below: Vec<CteDependency> = frame
                .cte_dependencies
                .values()
                .cloned()
                .collect::<Vec<CteDependency>>();

            if deps_in_one_level_below.is_empty() {
                return;
            }

            let mut resolved_dependencies = HashSet::new();

            for dep in deps_in_one_level_below {
                if !dep.is_used {
                    continue;
                }

                resolved_dependencies.extend(dep.deps.clone());
            }

            frame.cte_dependencies = HashMap::new();
            frame.cte_dependencies.insert(
                cte_name,
                CteDependency {
                    is_used: false,
                    deps: resolved_dependencies,
                },
            );

            frame.dependencies = HashSet::new();
        }
    }

    pub fn adjust_cte_dependencies(&mut self, cte_name: String) {
        // filter upper level deps
        let size = self.frames.iter().clone().len();

        let dependencies = self.frames[size - 1].dependencies.clone();
        let cte_dependencies = self.frames[size - 1].cte_dependencies.clone();

        let mut resolved_dependencies = HashSet::new();

        for dependency in dependencies {
            if cte_name.clone() == dependency.name {
                continue;
            }

            if cte_dependencies.contains_key(&dependency.name) {
                resolved_dependencies
                    .extend(cte_dependencies.get(&dependency.name).unwrap().clone().deps);
                continue;
            }

            resolved_dependencies.insert(dependency.clone());
        }

        if let Some(frame) = self.frames.last_mut() {
            frame.cte_dependencies.insert(
                cte_name.clone(),
                CteDependency {
                    is_used: false,
                    deps: resolved_dependencies,
                },
            );
            frame.dependencies = HashSet::new();
        }
    }

    pub fn collect_aliases(&mut self, old: &ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.is_main_body = old.is_main_body;
            frame.dependencies.extend(old.dependencies.clone());
            frame.cte_dependencies.extend(old.cte_dependencies.clone());

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

    pub fn default_database(&self) -> &Option<String> {
        &self.default_database
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
        ancestry.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(
            ancestry,
            vec![
                (
                    &ColumnMeta::new("c".to_string(), None),
                    &HashSet::from([
                        ColumnMeta::new(
                            "x".to_string(),
                            Some(DbTableMeta::new_default_dialect("t2".to_string())),
                        ),
                        ColumnMeta::new(
                            "y".to_string(),
                            Some(DbTableMeta::new_default_dialect("t1".to_string())),
                        ),
                        ColumnMeta::new(
                            "b".to_string(),
                            Some(DbTableMeta::new_default_dialect("t1".to_string())),
                        ),
                    ])
                ),
                (
                    &ColumnMeta::new("d".to_string(), None),
                    &HashSet::from([ColumnMeta::new(
                        "b".to_string(),
                        Some(DbTableMeta::new_default_dialect("t1".to_string())),
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
        ancestry.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(
            ancestry,
            vec![
                (
                    &ColumnMeta::new(
                        "c".to_string(),
                        Some(DbTableMeta::new_default_dialect("alias".to_string())),
                    ),
                    &HashSet::from([ColumnMeta::new(
                        "a".to_string(),
                        Some(DbTableMeta::new_default_dialect("table".to_string())),
                    ),])
                ),
                (
                    &ColumnMeta::new(
                        "d".to_string(),
                        Some(DbTableMeta::new_default_dialect("alias".to_string())),
                    ),
                    &HashSet::from([ColumnMeta::new(
                        "b".to_string(),
                        Some(DbTableMeta::new_default_dialect("table".to_string())),
                    ),])
                ),
            ]
        );
    }
}
