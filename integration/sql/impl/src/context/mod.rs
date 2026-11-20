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
    pub is_main_body: bool,
}

#[derive(Debug, Clone)]
pub struct CteDependency {
    // The CTE's name as written in its WITH clause, quote style included,
    // which decides how references to it fold.
    pub name: Ident,
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
            bare_dependencies: HashSet::new(),
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
    // For each SELECT being visited, innermost last: the names its FROM clause
    // introduces, keyed by `ident_key`, with the relation each one stands for.
    // An alias stands for itself, so the alias table resolves it; an unaliased
    // table stands for its full name (`orders` for `FROM raw.orders`).
    correlation_names: Vec<HashMap<String, Vec<Ident>>>,
    // Where each SELECT's scopes begin in `correlation_names`.
    select_scope_starts: Vec<usize>,
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
            select_scope_starts: Vec::new(),
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
            select_scope_starts: Vec::new(),
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
        self.frames
            .iter()
            .rev()
            .find_map(|frame| frame.cte_dependencies.get(&key))
            .map(|cte| cte.name.clone())
    }

    /// Makes a recursive CTE's own name resolvable inside its body, so a
    /// self-reference in another case resolves like one in the CTE's own
    /// case. `adjust_cte_dependencies` replaces the entry once the body has
    /// been visited.
    pub fn declare_recursive_cte(&mut self, cte_name: Ident) {
        let key = ident_key(&cte_name, self.dialect().as_base());
        if let Some(frame) = self.frames.last_mut() {
            frame.cte_dependencies.insert(
                key,
                CteDependency {
                    name: cte_name,
                    is_used: false,
                    deps: HashSet::new(),
                },
            );
        }
    }

    /// Opens the scope of the names a SELECT's FROM clause introduces,
    /// filled by `add_correlation_names` and closed by
    /// `pop_correlation_names`.
    pub fn push_correlation_names(&mut self) {
        self.select_scope_starts.push(self.correlation_names.len());
        self.correlation_names.push(HashMap::new());
    }

    /// Opens a scope for the members of a parenthesized join, which a
    /// LATERAL member sees as they are added. Closed by `pop_join_scope`.
    pub fn push_join_scope(&mut self) {
        self.correlation_names.push(HashMap::new());
    }

    pub fn pop_join_scope(&mut self) {
        self.correlation_names.pop();
    }

    /// Removes the scopes of the SELECT whose FROM clause is being visited,
    /// so a non-LATERAL derived table cannot bind to the items beside it;
    /// enclosing SELECTs stay visible. Undone by `restore_from_names`.
    pub fn hide_from_names(&mut self) -> Vec<HashMap<String, Vec<Ident>>> {
        match self.select_scope_starts.last() {
            Some(&start) => self.correlation_names.split_off(start),
            None => Vec::new(),
        }
    }

    pub fn restore_from_names(&mut self, hidden: Vec<HashMap<String, Vec<Ident>>>) {
        self.correlation_names.extend(hidden);
    }

    /// Adds `names` to the innermost scope, each an alias or an unaliased
    /// table's last name part, with the relation it stands for.
    pub fn add_correlation_names(&mut self, names: Vec<(Ident, Vec<Ident>)>) {
        let dialect = self.dialect().as_base();
        let keyed: Vec<(String, Vec<Ident>)> = names
            .into_iter()
            .map(|(name, relation)| (ident_key(&name, dialect), relation))
            .collect();
        if let Some(scope) = self.correlation_names.last_mut() {
            scope.extend(keyed);
        }
    }

    /// Closes the SELECT's scope, and any join scope a failed visit left open.
    pub fn pop_correlation_names(&mut self) {
        if let Some(start) = self.select_scope_starts.pop() {
            self.correlation_names.truncate(start);
        }
    }

    /// The relation a column qualifier names through the FROM clause of an
    /// enclosing SELECT, innermost first, compared under the dialect's
    /// folding, or None when no FROM clause in scope introduces it.
    pub fn correlation_named_by(&self, qualifier: &[Ident]) -> Option<Vec<Ident>> {
        let [name] = qualifier else {
            return None;
        };
        let key = ident_key(name, self.dialect().as_base());
        self.correlation_names
            .iter()
            .rev()
            .find_map(|scope| scope.get(&key))
            .cloned()
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
        let key = ident_key(table, self.dialect().as_base());
        for frame in self.frames.iter_mut().rev() {
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

    /// Like `is_table_alias`, but not counting the placeholder entries
    /// `With::visit` registers for CTE names, which map to an empty table.
    /// The alias table matches names without regard to quoting, so those
    /// entries also catch references that fold to a different name than the
    /// CTE; CTE references are resolved by `cte_named_by` instead.
    pub fn is_relation_alias(&self, name: &DbTableMeta) -> bool {
        let resolved = self.resolve_table(name);
        resolved != name && !resolved.name.is_empty()
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
        frame.is_main_body = old.is_main_body;
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
                // this can happen when selecting from subquery without alias
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
            frame.column_ancestry.extend(old.column_ancestry.drain());
            frame.dependencies.extend(old.dependencies);
            frame.bare_dependencies.extend(old.bare_dependencies);
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
            }

            frame.dependencies.extend(old.dependencies);
            frame.bare_dependencies.extend(old.bare_dependencies);
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

    pub fn collect_lower_nested_dependencies(&mut self, cte_name: Ident) {
        let cte_key = ident_key(&cte_name, self.dialect().as_base());
        if let Some(frame) = self.frames.last_mut() {
            // Not the CTE's own entry from `declare_recursive_cte`.
            let deps_in_one_level_below: Vec<CteDependency> = frame
                .cte_dependencies
                .iter()
                .filter(|(key, _)| **key != cte_key)
                .map(|(_, dep)| dep.clone())
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
                cte_key,
                CteDependency {
                    name: cte_name,
                    is_used: false,
                    deps: resolved_dependencies,
                },
            );

            frame.dependencies = HashSet::new();
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

            if reference_key == cte_key {
                if !recursive {
                    resolved_dependencies.insert(dependency.clone());
                }
                continue;
            }

            if let Some(cte) = cte_dependencies.get(&reference_key) {
                resolved_dependencies.extend(cte.deps.clone());
                continue;
            }

            resolved_dependencies.insert(dependency.clone());
        }

        if let Some(frame) = self.frames.last_mut() {
            frame.cte_dependencies.insert(
                cte_key,
                CteDependency {
                    name: cte_name,
                    is_used: false,
                    deps: resolved_dependencies,
                },
            );
            frame.dependencies = HashSet::new();
            frame.bare_dependencies = HashSet::new();
        }
    }

    pub fn collect_aliases(&mut self, old: &ContextFrame) {
        if let Some(frame) = self.frames.last_mut() {
            frame.is_main_body = old.is_main_body;
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
