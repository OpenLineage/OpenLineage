// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

mod alias_table;

use std::collections::{HashMap, HashSet};

use crate::dialect::{fold_ident, ident_key, CanonicalDialect};
use crate::lineage::*;
use alias_table::AliasTable;
use sqlparser::ast::Ident;
use sqlparser::dialect::Dialect;
#[cfg(test)]
use sqlparser::dialect::SnowflakeDialect;

type ColumnAncestors = HashSet<ColumnMeta>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableDependency {
    /// A single-identifier reference written without schema or database,
    /// which may resolve to a CTE in scope (or stay a real table with defaults).
    Bare(DbTableMeta),
    /// A schema/database-qualified reference or already-resolved physical table.
    Resolved(DbTableMeta),
}

impl TableDependency {
    fn table(&self) -> &DbTableMeta {
        match self {
            TableDependency::Bare(t) | TableDependency::Resolved(t) => t,
        }
    }

    fn into_table(self) -> DbTableMeta {
        match self {
            TableDependency::Bare(t) | TableDependency::Resolved(t) => t,
        }
    }

    /// The dialect-folded CTE lookup key if this dependency was written bare,
    /// or `None` if it was qualified (and therefore cannot name a CTE).
    fn cte_key(&self, dialect: &dyn Dialect) -> Option<String> {
        let TableDependency::Bare(table) = self else {
            return None;
        };
        let quoted = table
            .quote_style
            .as_ref()
            .and_then(|style| style.name)
            .is_some();
        Some(fold_ident(&table.name, quoted, dialect))
    }
}

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
    pub dependencies: HashSet<TableDependency>,
    // Keyed by `ident_key` of the CTE's name. Populated only on a `Query`'s
    // `WITH` frame and resolved when that `Query` finishes, so CTE registrations
    // never leak into enclosing frames.
    pub cte_dependencies: HashMap<String, CteDependency>,
    // The recursive CTE whose body is currently being visited in this frame
    // (`(ident_key, name)`), kept apart from `cte_dependencies` so a nested
    // CTE of the same folded name cannot replace it.
    recursive_cte: Option<(String, Ident)>,
    // Whether this frame's region is the main body of its statement, where
    // table references become inputs directly. Frame merges must never
    // overwrite the receiving frame's flag with the merged frame's.
    pub is_main_body: bool,
}

#[derive(Debug, Clone)]
pub struct CteDependency {
    // The CTE's name as written in its WITH clause, quote style included,
    // which decides how references to it fold.
    pub name: Ident,
    pub is_used: bool,
    pub deps: HashSet<TableDependency>,
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
            recursive_cte: None,
            is_main_body: true,
        }
    }

    pub fn coalesce(&mut self, old: &ContextFrame) {
        let mut result: HashMap<ColumnMeta, ColumnAncestors> = HashMap::new();

        for (col, ancestors) in &self.column_ancestry {
            let mut expanded = ColumnAncestors::new();
            for ancestor in ancestors {
                let traversed = Context::expand_ancestors(ancestor.clone(), old);
                if !traversed.is_empty() {
                    expanded.extend(traversed);
                }
            }
            result.insert(col.clone(), expanded);
        }

        self.column_ancestry = result;
        self.dependencies.extend(old.dependencies.iter().cloned());
    }
}

// Context struct serves as generic holder of all information we currently have about
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
    // For each SELECT being visited, innermost last: the names its own FROM
    // clause introduces, keyed by `ident_key`, with the relation each one
    // stands for. An alias stands for itself, so the alias table resolves it;
    // an unaliased table stands for its full name (`orders` for
    // `FROM raw.orders`).
    correlation_names: Vec<HashMap<String, Vec<Ident>>>,
}

impl<'a> Context<'a> {
    #[cfg(test)]
    pub fn default() -> Context<'a> {
        Context::new(&SnowflakeDialect, None, None)
    }

    pub fn new(
        dialect: &dyn CanonicalDialect,
        default_schema: Option<String>,
        default_database: Option<String>,
    ) -> Context<'_> {
        Context {
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            frames: vec![ContextFrame::new()],
            default_schema,
            default_database,
            dialect,
            column_id: 0,
            correlation_names: Vec::new(),
        }
    }

    pub fn table_meta(&self, idents: Vec<Ident>) -> DbTableMeta {
        DbTableMeta::new(
            idents,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        )
    }

    fn non_table_meta(
        &self,
        idents: Vec<Ident>,
        provided_namespace: bool,
        provided_field_schema: bool,
    ) -> DbTableMeta {
        DbTableMeta::new_with_namespace_and_schema(
            idents,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
            provided_namespace,
            provided_field_schema,
            false,
        )
    }

    // --- Table Lineage ---

    pub fn add_input(&mut self, table: Vec<Ident>) {
        let name = self.table_meta(table);
        if !self.is_table_alias(&name) {
            self.inputs.insert(name);
        }
    }

    /// `add_input` for a table reference that `cte_named_by` has already
    /// ruled out as a CTE, so only a real alias suppresses it.
    pub fn add_relation_input(&mut self, table: DbTableMeta) {
        if !self.is_relation_alias(&table) {
            self.inputs.insert(table);
        }
    }

    pub fn add_non_table_input(
        &mut self,
        table: Vec<Ident>,
        provided_namespace: bool,
        provided_field_schema: bool,
    ) {
        let name = self.non_table_meta(table, provided_namespace, provided_field_schema);
        if !self.is_table_alias(&name) {
            self.inputs.insert(name);
        }
    }

    pub fn add_output(&mut self, output: Vec<Ident>) {
        let name = self.table_meta(output);
        if !self.is_table_alias(&name) {
            self.outputs.insert(name);
        }
    }

    /// Registers an INSERT target as an output. Unlike
    /// [`Context::add_output`], the name is never treated as a table alias:
    /// CTE names are registered as aliases of an empty table, and an insert
    /// target can share its name with a CTE defined over the insert's
    /// source. The target is a real table written by the statement, outside
    /// the CTE's alias scope, so it must always be registered.
    pub fn add_insert_output(&mut self, output: Vec<Ident>) {
        let name = self.table_meta(output);
        self.outputs.insert(name);
    }

    pub fn move_input_to_output(&mut self, input: Vec<Ident>) {
        let name = self.table_meta(input);
        let output = self.resolve_table(&name).clone();
        self.inputs.remove(&output);
        self.outputs.insert(output);
    }

    pub fn add_non_table_output(
        &mut self,
        output: Vec<Ident>,
        provided_namespace: bool,
        provided_field_schema: bool,
    ) {
        let name = self.non_table_meta(output, provided_namespace, provided_field_schema);
        if !self.is_table_alias(&name) {
            self.outputs.insert(name);
        }
    }

    // --- Column Lineage ---

    pub fn add_column_ancestors(&mut self, column: ColumnMeta, mut ancestors: Vec<ColumnMeta>) {
        let Some(frame) = self.frames.last_mut() else {
            return;
        };

        for ancestor in &mut ancestors {
            if let Some(table) = &mut ancestor.origin {
                *table = frame.aliases.resolve_table(table).clone();
            }
        }

        frame
            .column_ancestry
            .entry(column)
            .and_modify(|x| x.extend(ancestors.drain(..)))
            .or_insert_with(|| HashSet::from_iter(ancestors));
    }

    // --- Context Manipulators ---

    pub fn push_frame(&mut self) {
        let mut frame = ContextFrame::new();
        if let Some(recent) = self.frames.last() {
            frame.table.clone_from(&recent.table);
            frame.column.clone_from(&recent.column);
            frame.is_main_body = recent.is_main_body;
        }
        self.frames.push(frame);
    }

    pub fn is_main(&self) -> bool {
        self.frames.last().is_some_and(|frame| frame.is_main_body)
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

    pub fn add_table_alias(&mut self, table: DbTableMeta, alias: Ident) {
        let alias = self.table_meta(vec![alias]);
        if let Some(frame) = self.frames.last_mut() {
            frame.aliases.add_table_alias(table, alias);
        }
    }

    pub fn add_table_dependency(&mut self, table: Vec<Ident>) {
        let is_bare = table.len() == 1;
        let meta = self.table_meta(table);
        let dep = if is_bare {
            TableDependency::Bare(meta)
        } else {
            TableDependency::Resolved(meta)
        };
        if let Some(frame) = self.frames.last_mut() {
            frame.dependencies.insert(dep);
        }
    }

    fn cte_by_key(&self, key: &str) -> Option<&Ident> {
        self.frames.iter().rev().find_map(|frame| {
            frame
                .recursive_cte
                .as_ref()
                .filter(|(k, _)| k == key)
                .map(|(_, name)| name)
                .or_else(|| frame.cte_dependencies.get(key).map(|cte| &cte.name))
        })
    }

    /// The name, as defined, of the CTE in scope that the table reference
    /// `name` points at, or None when it names a real table.
    ///
    /// Only a reference written without a schema or database can name a CTE,
    /// so `schema.c` never matches CTE `c`. The comparison follows the
    /// dialect's identifier folding (see `ident_key`), so `orders` names CTE
    /// `Orders` in Snowflake. Callers use the returned name in place of the
    /// reference, so the reference and the CTE's own columns share one table
    /// name downstream.
    pub fn cte_named_by(&self, name: &[Ident]) -> Option<Ident> {
        let [reference] = name else {
            return None;
        };
        let key = ident_key(reference, self.dialect.as_base());
        self.cte_by_key(&key).cloned()
    }

    /// Makes a recursive CTE's own name resolvable inside its body, so a
    /// self-reference in another case resolves like one in the CTE's own
    /// case. `adjust_cte_dependencies` removes it once the body has been
    /// visited.
    pub fn declare_recursive_cte(&mut self, cte_name: Ident) {
        let key = ident_key(&cte_name, self.dialect.as_base());
        if let Some(frame) = self.frames.last_mut() {
            frame.recursive_cte = Some((key, cte_name));
        }
    }

    /// Runs `f` with the current SELECT's FROM-clause correlation names in
    /// scope, guaranteeing the scope is popped even when `f` returns an error.
    pub fn with_correlation_names<T>(
        &mut self,
        names: Vec<(Ident, Vec<Ident>)>,
        f: impl FnOnce(&mut Self) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let dialect = self.dialect.as_base();
        self.correlation_names.push(
            names
                .into_iter()
                .map(|(name, relation)| (ident_key(&name, dialect), relation))
                .collect(),
        );
        let result = f(self);
        self.correlation_names.pop();
        result
    }

    /// Resolves the table qualifier of a compound column reference (`q.col`).
    ///
    /// A single-identifier qualifier binds in order:
    /// 1. A correlation name introduced by the current SELECT's FROM clause
    ///    (alias or unaliased table's last name part), matched under the
    ///    dialect's identifier folding.
    /// 2. A CTE in scope (matched under the dialect's folding), unless the
    ///    qualifier is already a real relation alias in scope or names a
    ///    non-matching FROM item in an enclosing SELECT.
    /// 3. Otherwise, the qualifier as written.
    pub fn resolve_column_qualifier(&self, qualifier: Vec<Ident>) -> DbTableMeta {
        let [name] = qualifier.as_slice() else {
            return self.table_meta(qualifier);
        };
        let key = ident_key(name, self.dialect.as_base());

        if let Some(relation) = self.correlation_names.last().and_then(|s| s.get(&key)) {
            return self.table_meta(relation.clone());
        }

        if let Some(cte) = self.cte_by_key(&key) {
            let as_written = self.table_meta(qualifier);
            if self.is_relation_alias(&as_written)
                || self
                    .correlation_names
                    .iter()
                    .rev()
                    .skip(1)
                    .find_map(|scope| scope.get(&key))
                    .is_some_and(|relation| relation.as_slice() != std::slice::from_ref(cte))
            {
                return as_written;
            }
            return self.table_meta(vec![cte.clone()]);
        }

        self.table_meta(qualifier)
    }

    /// `name` with a reference to a CTE in scope replaced by the CTE's name
    /// as defined; any other name is returned unchanged.
    pub fn resolve_cte_reference(&self, name: Vec<Ident>) -> Vec<Ident> {
        match self.cte_named_by(&name) {
            Some(cte) => vec![cte],
            None => name,
        }
    }

    /// Marks the CTE named `table` as used, so its dependencies are
    /// collected as inputs. `table` is the CTE's name as defined, as
    /// returned by [`Context::cte_named_by`], so the innermost CTE under its
    /// key is the one that lookup found.
    pub fn mark_table_as_used(&mut self, table: &Ident) {
        let key = ident_key(table, self.dialect.as_base());
        for frame in self.frames.iter_mut().rev() {
            if frame.recursive_cte.as_ref().is_some_and(|(k, _)| k == &key) {
                return;
            }
            if let Some(cte) = frame.cte_dependencies.get_mut(&key) {
                cte.is_used = true;
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

    /// Whether `name` is an alias for a relation, judged by the innermost
    /// alias entry for it. Unlike `is_table_alias`, this does not count the
    /// placeholder entries `With::visit` registers for CTE names, which map
    /// to an empty table: the alias table matches names without regard to
    /// quoting, so those entries also catch references that fold to a
    /// different name than the CTE. Only the first step is checked, because
    /// an alias of a CTE (`FROM orders AS o`) continues to that CTE's
    /// placeholder.
    pub fn is_relation_alias(&self, name: &DbTableMeta) -> bool {
        let qname = name.qualified_name();
        self.frames
            .iter()
            .rev()
            .find_map(|frame| frame.aliases.get_table_from_alias(&qname))
            .is_some_and(|table| !table.name.is_empty())
            && self.is_table_alias(name)
    }

    // --- Synthesize Attributes From Frames ---
    // These methods are used to retrieve information
    // from the narrower scope and allow the visitor
    // to compute synthesized attributes.

    pub fn coalesce(&mut self, old: ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.coalesce(&old);
        }
    }

    fn expand_ancestors(ancestor: ColumnMeta, old: &ContextFrame) -> Vec<ColumnMeta> {
        let mut result = Vec::new();
        let mut stack = vec![ancestor];
        let mut max_iter = 40;

        while let Some(current) = stack.pop() {
            max_iter -= 1;
            if max_iter <= 0 {
                break;
            }

            let column_ancestors = old
                .column_ancestry
                .get(&current)
                // try to find by name in case of column without origin
                // this can happen when selecting from subquery without alias.
                // An origin that names a CTE carries the CTE's name as
                // defined (references are replaced where they are read),
                // so no folded match is needed here - and one would be
                // wrong: a qualifier kept as written because it names an
                // alias or a real table (orders, o) must not be expanded
                // through a CTE that merely folds to the same key.
                .or_else(|| {
                    if current.origin.is_some() {
                        return None;
                    }
                    old.column_ancestry
                        .iter()
                        .find_map(|(col, anc)| (col.name == current.name).then_some(anc))
                });
            match column_ancestors {
                None => result.push(current),
                Some(ancestors) => {
                    for anc in ancestors {
                        if anc != &current {
                            stack.push(anc.clone());
                        }
                    }
                }
            }
        }

        result
    }

    pub fn collect(&mut self, mut old: ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            // Union ancestor sets so a merged frame (such as a parenthesized
            // set-operation arm) does not drop earlier arms' contributions.
            for (column, ancestors) in old.column_ancestry.drain() {
                frame
                    .column_ancestry
                    .entry(column)
                    .and_modify(|existing| existing.extend(ancestors.iter().cloned()))
                    .or_insert(ancestors);
            }
            frame.dependencies.extend(old.dependencies);
        }
    }

    /// Resolves a finished `Query`'s `WITH` registrations against its body and
    /// enclosing scopes before `body_frame` merges outward. Call with both
    /// `body_frame` and `with_frame` already popped so `self.frames` holds only
    /// enclosing scopes:
    ///
    /// - bare dependencies in `body_frame` that name a CTE in `with_frame` mark
    ///   that CTE used (needed when `is_main` is false and references record
    ///   dependencies instead of marking CTEs used directly);
    /// - used CTEs are resolved transitively through sibling CTEs;
    /// - at the main body, each resolved dependency either marks an enclosing
    ///   CTE used (if written bare and matching one) or becomes a statement
    ///   input; inside an enclosing CTE body it stays on `body_frame` as a
    ///   dependency (preserving bare vs. qualified) so the enclosing CTE
    ///   decides its fate;
    /// - unused CTEs contribute nothing, and `with_frame`'s CTE registrations
    ///   and aliases are dropped with `with_frame`.
    pub fn resolve_cte_scope(
        &mut self,
        mut with_frame: ContextFrame,
        body_frame: &mut ContextFrame,
        recursive: bool,
    ) {
        if with_frame.cte_dependencies.is_empty() {
            return;
        }

        let dialect = self.dialect.as_base();

        let own_references: Vec<String> = body_frame
            .dependencies
            .iter()
            .filter_map(|dep| dep.cte_key(dialect))
            .filter(|key| with_frame.cte_dependencies.contains_key(key))
            .collect();
        for key in own_references {
            if let Some(cte) = with_frame.cte_dependencies.get_mut(&key) {
                cte.is_used = true;
            }
        }

        let mut resolved: Vec<TableDependency> = Vec::new();
        let mut resolved_names: HashSet<String> = HashSet::new();
        let mut queue: Vec<String> = with_frame
            .cte_dependencies
            .iter()
            .filter(|(_, cte)| cte.is_used)
            .map(|(key, _)| key.clone())
            .collect();

        while let Some(key) = queue.pop() {
            if !resolved_names.insert(key.clone()) {
                continue;
            }
            if let Some(cte) = with_frame.cte_dependencies.get(&key) {
                for dep in &cte.deps {
                    // Backward sibling references are already expanded in
                    // definition order by `adjust_cte_dependencies`. Only
                    // `WITH RECURSIVE` allows forward references to a later
                    // sibling CTE; in a non-recursive WITH, a bare reference
                    // to a later sibling's name reads the physical table.
                    if recursive {
                        if let Some(dep_key) = dep.cte_key(dialect) {
                            if dep_key != key && with_frame.cte_dependencies.contains_key(&dep_key)
                            {
                                queue.push(dep_key);
                                continue;
                            }
                        }
                    }
                    resolved.push(dep.clone());
                }
            }
        }

        // Drop bare references in `body_frame` that resolved to this WITH's
        // CTEs before inserting `resolved` back, so a real table sharing a
        // non-recursive CTE's name is preserved.
        body_frame.dependencies.retain(|dep| {
            !dep.cte_key(dialect)
                .is_some_and(|k| resolved_names.contains(&k))
        });

        let enclosing_is_main = self.is_main();
        for dep in resolved {
            if enclosing_is_main {
                if let Some(dep_key) = dep.cte_key(self.dialect.as_base()) {
                    if let Some(outer_cte) = self.cte_by_key(&dep_key).cloned() {
                        self.mark_table_as_used(&outer_cte);
                        continue;
                    }
                }
                self.inputs.insert(dep.into_table());
            } else {
                body_frame.dependencies.insert(dep);
            }
        }
    }

    pub fn collect_inputs(&mut self, frame: &ContextFrame) {
        self.inputs
            .extend(frame.dependencies.iter().map(|d| d.table().clone()));
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
            }

            frame.dependencies.extend(old.dependencies);
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
                if let Some(origin) = &column_meta.origin {
                    if frame.aliases.is_table_alias(origin) && *origin == from_table {
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

    /// Resolves the dependencies collected for CTE `cte_name`. `recursive` is
    /// whether the CTE's own name is in scope inside its body: true for
    /// `WITH RECURSIVE`, and in dialects such as Snowflake where any CTE may
    /// refer to itself. Otherwise a reference to that name is the real table.
    /// `always_used` is true for data-modifying CTEs (`INSERT`/`UPDATE`/`DELETE`/
    /// `MERGE`), which execute and consume their inputs even when the outer
    /// query never reads the CTE.
    pub fn adjust_cte_dependencies(&mut self, cte_name: Ident, recursive: bool, always_used: bool) {
        let dialect = self.dialect.as_base();
        let cte_key = ident_key(&cte_name, dialect);
        let Some(frame) = self.frames.last_mut() else {
            return;
        };

        let mut resolved = HashSet::new();

        for dependency in std::mem::take(&mut frame.dependencies) {
            match dependency.cte_key(dialect) {
                // Only references written without a schema or database can name
                // a CTE; `raw.orders` and `public.orders` stay real tables.
                None => {
                    resolved.insert(dependency);
                }
                Some(dep_key) => {
                    // Earlier sibling CTEs in the same WITH clause resolve
                    // transitively here; nested WITH clauses inside the CTE
                    // body were already resolved when their own `Query` exited.
                    if let Some(cte) = frame.cte_dependencies.get(&dep_key) {
                        resolved.extend(cte.deps.iter().cloned());
                    } else if dep_key != cte_key || !recursive {
                        resolved.insert(dependency);
                    }
                }
            }
        }

        frame.recursive_cte = None;
        frame.cte_dependencies.insert(
            cte_key,
            CteDependency {
                name: cte_name,
                is_used: always_used,
                deps: resolved,
            },
        );
    }

    /// Merges only the table dependencies of a popped frame, discarding its
    /// column ancestry and local aliases.
    pub fn collect_dependencies(&mut self, old: ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.dependencies.extend(old.dependencies);
        }
    }

    pub fn collect_aliases(&mut self, old: &ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.dependencies.extend(old.dependencies.iter().cloned());
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
