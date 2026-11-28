// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

mod alias_table;

use std::collections::{HashMap, HashSet};

use crate::dialect::CanonicalDialect;
use crate::lineage::*;
use alias_table::AliasTable;
use sqlparser::ast::Ident;
use sqlparser::dialect::{
    BigQueryDialect, DatabricksDialect, Dialect, DuckDbDialect, HiveDialect, MsSqlDialect,
    MySqlDialect, PostgreSqlDialect, RedshiftSqlDialect, SQLiteDialect, SnowflakeDialect,
};

type ColumnAncestors = HashSet<ColumnMeta>;

/// The stored form of an identifier under the active dialect: a quoted
/// identifier is stored exactly as written, while an unquoted one is
/// folded - to lower case by Postgres and Redshift, to upper case by
/// Snowflake and the SQL standard. Redshift folds even quoted identifiers
/// to lower case unless the session enables
/// `enable_case_sensitive_identifier`, which the parser is not told, so
/// every Redshift identifier canonicalizes to lower case.
fn canonical_ident(value: &str, quoted: bool, dialect: &dyn Dialect) -> String {
    if dialect.is::<RedshiftSqlDialect>() {
        value.to_ascii_lowercase()
    } else if quoted {
        value.to_string()
    } else if dialect.is::<PostgreSqlDialect>() {
        value.to_ascii_lowercase()
    } else {
        value.to_ascii_uppercase()
    }
}

/// The name an identifier denotes under the dialect, as a string that is
/// equal for two identifiers exactly when they name the same relation. CTEs
/// are keyed by it, so `orders` finds CTE `Orders` in Snowflake (both
/// ORDERS) and unquoted `C` finds CTE `"c"` in Postgres (both c), while
/// Postgres `"C"` and `C` stay two names and two CTEs.
///
/// BigQuery, Databricks, Hive, DuckDB, SQL Server and SQLite compare names
/// without regard to case, quoted or not. MySQL compares them exactly, as
/// it does table aliases on Unix under its default configuration.
fn ident_key(ident: &Ident, dialect: &dyn Dialect) -> String {
    if dialect.is::<MySqlDialect>() {
        return ident.value.clone();
    }
    if dialect.is::<BigQueryDialect>()
        || dialect.is::<DatabricksDialect>()
        || dialect.is::<HiveDialect>()
        || dialect.is::<DuckDbDialect>()
        || dialect.is::<MsSqlDialect>()
        || dialect.is::<SQLiteDialect>()
    {
        return ident.value.to_ascii_lowercase();
    }
    canonical_ident(&ident.value, ident.quote_style.is_some(), dialect)
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
    pub dependencies: HashSet<DbTableMeta>,
    // Dependencies written without a schema or database. Only these can name a
    // CTE: defaults make `orders` and `public.orders` the same `DbTableMeta`.
    pub bare_dependencies: HashSet<DbTableMeta>,
    // Keyed by `ident_key` of the CTE's name.
    pub cte_dependencies: HashMap<String, CteDependency>,
    // Recursive CTEs whose body is being visited, keyed like
    // `cte_dependencies`, so a self-reference resolves to the CTE's name.
    // Kept apart from `cte_dependencies` so a nested CTE of the same name
    // cannot replace the entry.
    recursive_ctes: HashMap<String, Ident>,
    // Whether this frame's region is the main body of its statement, where
    // table references become inputs directly. The flag belongs to the
    // frame's own region: Query::visit sets it on the frames it creates,
    // and frame merges (collect, coalesce, collect_aliases,
    // collect_with_table) must never overwrite the receiving frame's flag
    // with the merged frame's. A merged subquery frame is routinely not a
    // main body (a query with a WITH unsets the flag on its own frames);
    // copying its flag up would silently demote the rest of the enclosing
    // body - later joins, say - to dependency recording.
    pub is_main_body: bool,
}

#[derive(Debug, Clone)]
pub struct CteDependency {
    // The CTE's name as written in its WITH clause, quote style included,
    // which decides how references to it fold.
    pub name: Ident,
    pub is_used: bool,
    pub deps: HashSet<DbTableMeta>,
    // The subset of `deps` that was written without a schema or database
    // at the point it was recorded. Only those can name a CTE of an
    // enclosing scope: a reference written `sch.c` is the real table
    // even when a CTE called `c` is in scope, so it must never be
    // resolved through one. Tracked separately because DbTableMeta
    // values cannot carry the distinction - defaults make a bare `c`
    // and an explicit `public.c` the same value.
    pub bare_deps: HashSet<DbTableMeta>,
}

impl ContextFrame {
    fn new() -> Self {
        ContextFrame {
            table: None,
            column: None,
            aliases: AliasTable::new(),
            column_ancestry: HashMap::new(),
            dependencies: HashSet::new(),
            bare_dependencies: HashSet::new(),
            cte_dependencies: HashMap::new(),
            recursive_ctes: HashMap::new(),
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
    // For each SELECT being visited, innermost last: the names its own FROM
    // clause introduces, keyed by `ident_key`.
    correlation_names: Vec<HashMap<String, CorrelationName>>,
}

/// A name a FROM item introduces for column qualifiers.
#[derive(Debug, Clone)]
pub struct CorrelationName {
    /// The relation the name stands for: an alias stands for itself, so
    /// the alias table of its own SELECT resolves it; an unaliased table
    /// stands for its full name (`orders` for `FROM raw.orders`).
    pub relation: Vec<Ident>,
    pub is_alias: bool,
    /// For an alias of a table or CTE, that table's name as the FROM item
    /// wrote it, CTE references resolved (`u` for `FROM u AS c`). None for
    /// an unaliased table, and for the alias of a derived table, a table
    /// function or anything else whose columns are keyed by the alias.
    pub renames: Option<Vec<Ident>>,
}

/// The identifier a recorded table reference was written with, rebuilt
/// from the parts `DbTableMeta` keeps: its name and quote style.
fn ident_of(table: &DbTableMeta) -> Ident {
    let mut ident = Ident::new(table.name.clone());
    ident.quote_style = table.quote_style.as_ref().and_then(|style| style.name);
    ident
}

/// The CTE key of a recorded table reference: the key of the
/// identifier it was written with, under the dialect's folding (see
/// `ident_key`), so it matches the key the CTE was registered under.
fn cte_key_of(table: &DbTableMeta, dialect: &dyn Dialect) -> String {
    ident_key(&ident_of(table), dialect)
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
            correlation_names: Vec::new(),
        }
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

    /// `add_input` for a table reference that `cte_named_by` has already
    /// ruled out as a CTE, so only a real alias suppresses it.
    pub fn add_relation_input(&mut self, table: Vec<Ident>) {
        let name = DbTableMeta::new(
            table,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        );
        if !self.is_relation_alias(&name) {
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

    /// Registers an INSERT target as an output. Unlike
    /// [`Context::add_output`], the name is never treated as a table alias:
    /// CTE names are registered as aliases of an empty table, and an insert
    /// target can share its name with a CTE defined over the insert's
    /// source. The target is a real table written by the statement, outside
    /// the CTE's alias scope, so it must always be registered.
    pub fn add_insert_output(&mut self, output: Vec<Ident>) {
        let name = DbTableMeta::new(
            output,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        );
        self.outputs.insert(name);
    }

    pub fn move_input_to_output(&mut self, input: Vec<Ident>) {
        let name = DbTableMeta::new(
            input,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        );
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

    /// Like `add_column_ancestors`, for ancestors whose tables are already
    /// resolved: they are recorded as they are, not looked up again in the
    /// alias table of the SELECT being visited. A qualifier that names an
    /// enclosing SELECT's FROM item is resolved through that FROM item, and
    /// its table must not be taken by an alias of the nested SELECT that
    /// happens to share its name: in
    /// `SELECT (SELECT c.id FROM t AS u) FROM u AS c`, `c.id` is `u.id`,
    /// not `t.id`.
    pub fn add_resolved_column_ancestors(
        &mut self,
        column: ColumnMeta,
        ancestors: Vec<ColumnMeta>,
    ) {
        if let Some(frame) = self.frames.last_mut() {
            frame
                .column_ancestry
                .entry(column)
                .or_default()
                .extend(ancestors);
        }
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
        let is_bare = table.len() == 1;
        let name = DbTableMeta::new(
            table,
            self.dialect,
            self.default_schema.clone(),
            self.default_database.clone(),
        );

        if let Some(frame) = self.frames.last_mut() {
            if is_bare {
                frame.bare_dependencies.insert(name.clone());
            }
            frame.dependencies.extend(vec![name]);
        }
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
        let key = ident_key(reference, self.dialect().as_base());
        self.frames.iter().rev().find_map(|frame| {
            frame
                .recursive_ctes
                .get(&key)
                .or_else(|| frame.cte_dependencies.get(&key).map(|cte| &cte.name))
                .cloned()
        })
    }

    /// Makes a recursive CTE's own name resolvable inside its body, so a
    /// self-reference in another case resolves like one in the CTE's own
    /// case. `adjust_cte_dependencies` removes it once the body has been
    /// visited.
    pub fn declare_recursive_cte(&mut self, cte_name: Ident) {
        let key = ident_key(&cte_name, self.dialect().as_base());
        if let Some(frame) = self.frames.last_mut() {
            frame.recursive_ctes.insert(key, cte_name);
        }
    }

    /// Opens the scope of a SELECT with the names its FROM clause
    /// introduces, each an alias or an unaliased table's last name part,
    /// with the relation it stands for. Closed by `pop_correlation_names`.
    pub fn push_correlation_names(&mut self, names: Vec<(Ident, CorrelationName)>) {
        let dialect = self.dialect.as_base();
        self.correlation_names.push(
            names
                .into_iter()
                .map(|(name, binding)| (ident_key(&name, dialect), binding))
                .collect(),
        );
    }

    pub fn pop_correlation_names(&mut self) {
        self.correlation_names.pop();
    }

    /// The table a qualifier names through the nearest FROM clause of an
    /// enclosing SELECT that introduces it, compared under the dialect's
    /// folding, or None when none does; `correlated_table` covers the
    /// SELECT being visited. The table comes from the FROM item alone, not
    /// from the alias table, which an alias of a nested SELECT with the same
    /// name could answer: a table alias is the table it renames (the CTE's
    /// table for `FROM c AS c`), any other alias keeps its own name for its
    /// own columns, and an unaliased item is the table it names.
    pub fn enclosing_table(&self, qualifier: &[Ident]) -> Option<DbTableMeta> {
        let [name] = qualifier else {
            return None;
        };
        let key = ident_key(name, self.dialect().as_base());
        let binding = self
            .correlation_names
            .iter()
            .rev()
            .skip(1)
            .find_map(|scope| scope.get(&key))?;
        Some(match (&binding.renames, binding.is_alias) {
            (Some(table), _) => self.relation_table(table.clone()),
            (None, true) => DbTableMeta::new(
                binding.relation.clone(),
                self.dialect,
                self.default_schema.clone(),
                self.default_database.clone(),
            ),
            (None, false) => self.relation_table(binding.relation.clone()),
        })
    }

    /// `name` with a reference to a CTE in scope replaced by the CTE's name
    /// as defined; any other name is returned unchanged.
    pub fn resolve_cte_reference(&self, name: Vec<Ident>) -> Vec<Ident> {
        match self.cte_named_by(&name) {
            Some(cte) => vec![cte],
            None => name,
        }
    }

    /// The table under which a CTE's columns are collected and looked up:
    /// its name as defined, without the default schema or database. A real
    /// table is always named with them, so `public.c` under default schema
    /// `public` never shares the key of CTE `c`.
    pub fn cte_table(&self, cte: Ident) -> DbTableMeta {
        DbTableMeta::new(vec![cte], self.dialect, None, None)
    }

    /// The table a column qualifier names through the FROM clause of the
    /// SELECT being visited, compared under the dialect's folding, or None
    /// when that FROM clause does not introduce it. Enclosing SELECTs are not
    /// consulted, so a qualifier in a nested query that names an outer FROM
    /// item is resolved as it is on `main`. Whether the name is an alias
    /// comes from that FROM clause, not from the alias table, which also
    /// holds the aliases of enclosing queries: an alias, a derived table's
    /// included, keeps its own name under the defaults, and an unaliased
    /// reference to a CTE is the CTE's table.
    pub fn correlated_table(&self, qualifier: &[Ident]) -> Option<DbTableMeta> {
        let [name] = qualifier else {
            return None;
        };
        let key = ident_key(name, self.dialect().as_base());
        let binding = self.correlation_names.last()?.get(&key)?;
        if binding.is_alias {
            return Some(DbTableMeta::new(
                binding.relation.clone(),
                self.dialect,
                self.default_schema.clone(),
                self.default_database.clone(),
            ));
        }
        Some(self.relation_table(binding.relation.clone()))
    }

    /// The table `name` refers to: the CTE's table when `name` is a
    /// reference to a CTE in scope, and otherwise `name` under the default
    /// schema and database.
    pub fn relation_table(&self, name: Vec<Ident>) -> DbTableMeta {
        match self.cte_named_by(&name) {
            Some(cte) => self.cte_table(cte),
            None => DbTableMeta::new(
                name,
                self.dialect,
                self.default_schema.clone(),
                self.default_database.clone(),
            ),
        }
    }

    /// Marks the CTE named `table` as used, so its dependencies are
    /// collected as inputs. `table` is the CTE's name as defined, as
    /// returned by [`Context::cte_named_by`], so the innermost CTE under its
    /// key is the one that lookup found.
    pub fn mark_table_as_used(&mut self, table: &Ident) {
        let key = ident_key(table, self.dialect().as_base());
        for frame in self.frames.iter_mut().rev() {
            if frame.recursive_ctes.contains_key(&key) {
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
        self.is_table_alias(name)
            && self
                .frames
                .iter()
                .rev()
                .find_map(|frame| frame.aliases.get_table_from_alias(name.qualified_name()))
                .is_some_and(|table| !table.name.is_empty())
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
                }
            }
            result.insert(col.clone(), expanded);
        }

        frame.column_ancestry = result;
        frame.dependencies.extend(old.dependencies);
        frame.bare_dependencies.extend(old.bare_dependencies);
    }

    fn expand_ancestors(ancestor: ColumnMeta, old: &ContextFrame) -> Vec<ColumnMeta> {
        let mut result = Vec::new();
        let mut stack = Vec::new();
        let mut max_iter = 40;

        stack.push(ancestor.clone());

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
                    old.column_ancestry.iter().find_map(|(col, anc)| {
                        if col.name == current.name && current.origin.is_none() {
                            Some(anc)
                        } else {
                            None
                        }
                    })
                });
            if column_ancestors.is_none() {
                result.push(current.clone());
                continue;
            }

            if let Some(ancestors) = column_ancestors {
                for anc in ancestors {
                    // avoid infinite loop
                    if anc != &current {
                        stack.push(anc.clone());
                    }
                }
            }
        }

        result
    }

    pub fn collect(&mut self, mut old: ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            // Union the ancestor sets of columns both frames know: a
            // merged frame can hold another arm's or subquery's
            // contribution to the same output column (a parenthesized
            // set-operation arm, say), and replacing the set would
            // silently drop the earlier contributions.
            for (column, ancestors) in old.column_ancestry.drain() {
                frame
                    .column_ancestry
                    .entry(column)
                    .and_modify(|existing| existing.extend(ancestors.iter().cloned()))
                    .or_insert(ancestors);
            }
            frame.dependencies.extend(old.dependencies);
            frame.bare_dependencies.extend(old.bare_dependencies);
            // CTE registrations travel up with the frame, so the used
            // dependencies of CTEs defined in nested queries still reach
            // collect_inputs. Merges flow from inner scopes outward, so a
            // registration already present - an outer scope's - wins over
            // a same-named nested one.
            for (name, dep) in old.cte_dependencies {
                frame.cte_dependencies.entry(name).or_insert(dep);
            }
        }
    }

    /// Resolves the CTE registrations of a frame popped from a finished
    /// subquery - a derived table, an expression subquery, a CTE body -
    /// before the frame is merged into the enclosing query. A CTE is
    /// visible only inside its own query, so its registration must not
    /// travel up as a live registration: left in the enclosing frame it
    /// captures later references to real tables of the same name. What
    /// travels up instead is the resolved outcome:
    ///
    /// - the real tables a used CTE read become plain dependencies of
    ///   the frame (references between the subquery's own CTEs are
    ///   resolved transitively first);
    /// - a dependency that was written bare and names a CTE of an
    ///   enclosing scope is a reference to that CTE: at the main body
    ///   the enclosing registration is marked used and the dependency
    ///   itself is dropped, so the enclosing CTE's own inputs are
    ///   collected; inside another CTE body the dependency travels up
    ///   as a bare dependency instead, because the enclosing CTE may
    ///   itself go unused;
    /// - unused CTEs contribute nothing, as at statement level.
    ///
    /// Call this with the subquery's frame already popped: the frames
    /// still on the stack are the enclosing scopes.
    pub fn resolve_subquery_cte_dependencies(&mut self, frame: &mut ContextFrame) {
        if frame.cte_dependencies.is_empty() {
            return;
        }

        // A reference to one of this frame's own CTEs from a region
        // that records dependencies - anywhere outside the main body,
        // where a table reference cannot mark the CTE used directly -
        // arrives as a bare dependency naming it. That is a use of
        // the CTE: without this step the name would travel up as a
        // plain table dependency and the CTE's own inputs would be
        // lost. (The tail of this function then drops those bare
        // dependencies, as it does for every resolved CTE.)
        let dialect = self.dialect.as_base();
        let own_references: Vec<String> = frame
            .bare_dependencies
            .iter()
            .filter(|table| {
                frame
                    .cte_dependencies
                    .contains_key(&cte_key_of(table, dialect))
            })
            .map(|table| cte_key_of(table, dialect))
            .collect();
        for name in own_references {
            if let Some(dep) = frame.cte_dependencies.get_mut(&name) {
                dep.is_used = true;
            }
        }

        // Resolve the used registrations transitively: a used CTE whose
        // dependency names a sibling CTE of the same subquery pulls that
        // CTE's dependencies in, whether or not the sibling was itself
        // marked used.
        let mut resolved: Vec<(DbTableMeta, bool)> = Vec::new();
        let mut resolved_names: HashSet<String> = HashSet::new();
        let mut queue: Vec<String> = frame
            .cte_dependencies
            .iter()
            .filter(|(_, dep)| dep.is_used)
            .map(|(name, _)| name.clone())
            .collect();
        while let Some(name) = queue.pop() {
            if !resolved_names.insert(name.clone()) {
                continue;
            }
            if let Some(dep) = frame.cte_dependencies.get(&name) {
                for table in &dep.deps {
                    let table_key = cte_key_of(table, dialect);
                    // A dependency naming the CTE being resolved is
                    // the real table of that name, not a reference to
                    // the CTE: adjust_cte_dependencies kept it for
                    // exactly that reason (a non-recursive CTE's own
                    // name is not in scope in its body, and a recursive
                    // one's self-reference is dropped there instead).
                    // Queueing it would resolve the table away and
                    // lose the input.
                    if dep.bare_deps.contains(table)
                        && table_key != name
                        && frame.cte_dependencies.contains_key(&table_key)
                    {
                        queue.push(table_key);
                    } else {
                        resolved.push((table.clone(), dep.bare_deps.contains(table)));
                    }
                }
            }
        }

        frame.cte_dependencies.clear();

        // A plain dependency of the frame that was written bare and
        // names one of the resolved CTEs was a reference to it, from a
        // region that records dependencies; the CTE's own dependencies,
        // resolved above, already account for it. A qualified reference
        // of the same name is a real table and stays. This runs before
        // the resolved tables are distributed below: a resolved table
        // that shares a resolved CTE's name (the real table a
        // non-recursive CTE reads under its own name) is an input in
        // its own right and must not be dropped as a reference.
        let resolved_bare: Vec<DbTableMeta> = frame
            .bare_dependencies
            .iter()
            .filter(|table| resolved_names.contains(&cte_key_of(table, dialect)))
            .cloned()
            .collect();
        for table in resolved_bare {
            frame.dependencies.remove(&table);
            frame.bare_dependencies.remove(&table);
        }

        // Marking an enclosing CTE used is only sound at the main
        // body: inside another CTE body the enclosing CTE may itself
        // go unused, and marking it would collect its inputs anyway.
        // There the dependency travels up as a (bare) dependency
        // instead, so it is resolved when the enclosing CTE is
        // adjusted - and dropped if that CTE is recursive or unused.
        // Keeping was_bare in that case matters: a bare dependency is
        // the only kind adjust_cte_dependencies can recognise as a
        // CTE reference rather than a schema-qualified real table.
        let enclosing_is_main = self.is_main();
        for (table, was_bare) in resolved {
            let enclosing_cte = if enclosing_is_main && was_bare {
                self.cte_named_by(&[ident_of(&table)])
            } else {
                None
            };
            if let Some(cte) = enclosing_cte {
                self.mark_table_as_used(&cte);
            } else {
                if was_bare {
                    frame.bare_dependencies.insert(table.clone());
                }
                frame.dependencies.insert(table);
            }
        }
    }

    /// CTE registrations never outlive their statement: collect() carries
    /// nested registrations up to the global frame, so drop them there
    /// once the statement's inputs have been collected, lest a later
    /// statement mistake them for its own CTEs. Only the global frame
    /// is cleared: Statement::visit also runs for statements nested in
    /// a query body (a WITH wrapping an INSERT visits the INSERT as its
    /// own statement), and at that inner tail the enclosing statement's
    /// registrations live in deeper frames and must survive. Registrations
    /// reach the global frame when a statement's frame merges complete;
    /// a visit error aborts the whole parse instead (the caller
    /// propagates it and drops the Context), so a frame leaked by an
    /// early return never carries registrations into another statement.
    pub fn clear_global_cte_dependencies(&mut self) {
        if let Some(frame) = self.frames.first_mut() {
            frame.cte_dependencies.clear();
        }
    }

    pub fn collect_inputs(
        &mut self,
        cte_deps: HashMap<String, CteDependency>,
        deps: HashSet<DbTableMeta>,
        bare_deps: HashSet<DbTableMeta>,
    ) {
        if cte_deps.is_empty() {
            for table in deps {
                self.inputs.insert(table.clone());
            }
            return;
        }

        for value in cte_deps.values() {
            if !value.is_used {
                continue;
            }

            for table in &value.deps {
                self.inputs.insert(table.clone());
            }
        }

        // Dependencies that do not name a CTE are inputs in their own
        // right - a predicate subquery reading another table, say, whose
        // frame was merged into the statement's. Only bare dependencies
        // can name a CTE (see ContextFrame::bare_dependencies); one that
        // does is not itself an input - it resolves through the CTE,
        // whose own dependencies were collected above when it is used.
        for table in deps {
            if bare_deps.contains(&table)
                && cte_deps.contains_key(&cte_key_of(&table, self.dialect.as_base()))
            {
                continue;
            }
            self.inputs.insert(table);
        }
    }

    /// Collects the frame of a CTE body or derived table as the columns of
    /// `from_table`. `is_cte_body` says that `from_table` is the CTE being
    /// defined, so an ancestor in `from_table` itself refers to that name
    /// as it stood before this definition rather than to the body, and is
    /// not taken as an ancestry of its own. A CTE's table carries no default
    /// schema, so the alias table, whose entries do, cannot tell this.
    pub fn collect_with_table(
        &mut self,
        mut old: ContextFrame,
        from_table: DbTableMeta,
        is_cte_body: bool,
    ) {
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
                Context::remove_circular_deps(&old_ancestry, frame, from_table, is_cte_body);

            if !removed_circular_deps.is_empty() {
                frame.column_ancestry.extend(old_ancestry);
            }

            frame.dependencies.extend(old.dependencies);
            frame.bare_dependencies.extend(old.bare_dependencies);
            frame.cte_dependencies.extend(old.cte_dependencies);
        }
    }

    fn remove_circular_deps(
        old_ancestry: &HashMap<ColumnMeta, ColumnAncestors>,
        frame: &ContextFrame,
        from_table: DbTableMeta,
        is_cte_body: bool,
    ) -> HashMap<ColumnMeta, ColumnAncestors> {
        let mut removed_circular_deps: HashMap<ColumnMeta, ColumnAncestors> = HashMap::new();

        for (key_column_meta, column_ancestors) in old_ancestry.iter() {
            for column_meta in column_ancestors {
                if let Some(origin) = column_meta.origin.clone() {
                    if origin == from_table
                        && (is_cte_body || frame.aliases.is_table_alias(&origin))
                    {
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

    /// Replaces the dependencies of a CTE whose body has its own WITH by the
    /// tables of the nested CTEs it uses. They are left as resolved
    /// dependencies, so `adjust_cte_dependencies` keeps them, and the nested
    /// CTEs, whose scope has ended, are dropped.
    pub fn collect_lower_nested_dependencies(&mut self) {
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
            frame.dependencies = resolved_dependencies;
            frame.bare_dependencies = HashSet::new();
        }
    }

    /// Resolves the dependencies collected for CTE `cte_name`. `recursive` is
    /// whether the CTE's own name is in scope inside its body: true for
    /// `WITH RECURSIVE`, and in dialects such as Snowflake where any CTE may
    /// refer to itself. Otherwise a reference to that name is the real table.
    pub fn adjust_cte_dependencies(&mut self, cte_name: Ident, recursive: bool) {
        let dialect = self.dialect().as_base();
        let cte_key = ident_key(&cte_name, dialect);
        // filter upper level deps
        let size = self.frames.iter().clone().len();

        let dependencies = self.frames[size - 1].dependencies.clone();
        let cte_dependencies = self.frames[size - 1].cte_dependencies.clone();

        let bare_dependencies = self.frames[size - 1].bare_dependencies.clone();

        let mut resolved_dependencies = HashSet::new();
        let mut resolved_bare_dependencies = HashSet::new();

        // A used CTE nested in the body under the same key is replaced by
        // this one below, so its tables are carried over.
        if let Some(nested) = cte_dependencies.get(&cte_key).filter(|cte| cte.is_used) {
            resolved_dependencies.extend(nested.deps.clone());
            resolved_bare_dependencies.extend(nested.bare_deps.clone());
        }

        for dependency in dependencies {
            // CTEs are keyed by their bare alias, so only a reference written
            // without a schema or database can point at one; `raw.orders` and
            // `public.orders` stay real tables even inside CTE `orders`.
            if !bare_dependencies.contains(&dependency) {
                resolved_dependencies.insert(dependency.clone());
                continue;
            }

            // The dependency's name as written, so it folds like the CTE's.
            let reference = Ident {
                value: dependency.name.clone(),
                quote_style: dependency.quote_style.as_ref().and_then(|style| style.name),
                span: cte_name.span,
            };

            let reference_key = ident_key(&reference, dialect);

            // A CTE nested in the body shadows the CTE's own name, so
            // `C AS (WITH c AS (...) SELECT id FROM c)` reads the nested `c`.
            if let Some(cte) = cte_dependencies.get(&reference_key) {
                resolved_dependencies.extend(cte.deps.clone());
                resolved_bare_dependencies.extend(cte.bare_deps.clone());
                continue;
            }

            if reference_key == cte_key {
                if !recursive {
                    resolved_dependencies.insert(dependency.clone());
                    resolved_bare_dependencies.insert(dependency.clone());
                }
                continue;
            }

            resolved_dependencies.insert(dependency.clone());
            resolved_bare_dependencies.insert(dependency);
        }

        if let Some(frame) = self.frames.last_mut() {
            frame.recursive_ctes.remove(&cte_key);
            frame.cte_dependencies.insert(
                cte_key,
                CteDependency {
                    name: cte_name,
                    is_used: false,
                    deps: resolved_dependencies,
                    bare_deps: resolved_bare_dependencies,
                },
            );
            frame.dependencies = HashSet::new();
            frame.bare_dependencies = HashSet::new();
        }
    }

    /// Merges only the table dependencies of a popped frame, without its
    /// CTE and alias scope. Used for frames whose scope ends with them -
    /// a multi-table insert's WHEN predicate, say: the tables it reads are
    /// inputs of the statement, but a CTE it defines is visible only
    /// inside the predicate. Carrying that CTE's registration into the
    /// statement frame (as collect_aliases would) makes later, unrelated
    /// references to a real table of the same name resolve to the CTE and
    /// vanish from the inputs. The resolved dependencies of CTEs the frame
    /// did use are carried as plain dependencies - they were resolved
    /// against the frame's own scope, so they are real tables - but the
    /// CTE registrations themselves are not.
    pub fn collect_dependencies(&mut self, old: &ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.dependencies.extend(old.dependencies.clone());
            frame
                .bare_dependencies
                .extend(old.bare_dependencies.clone());
            for value in old.cte_dependencies.values() {
                if value.is_used {
                    frame.dependencies.extend(value.deps.clone());
                }
            }
        }
    }

    pub fn collect_aliases(&mut self, old: &ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.dependencies.extend(old.dependencies.clone());
            frame
                .bare_dependencies
                .extend(old.bare_dependencies.clone());
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
        context.collect_with_table(
            frame,
            DbTableMeta::new_default_dialect("alias".to_string()),
            false,
        );

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
