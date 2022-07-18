// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

mod bigquery;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub use bigquery::BigQueryDialect;
use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use sqlparser::ast::{
    Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor, With,
};
use sqlparser::dialect::{
    AnsiDialect, Dialect, GenericDialect, HiveDialect, MsSqlDialect, MySqlDialect,
    PostgreSqlDialect, SQLiteDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;

pub trait CanonicalDialect: Dialect {
    fn canonical_name(&self, name: String) -> Option<String>;
    fn as_base(&self) -> &dyn Dialect;
}

impl<T: Dialect> CanonicalDialect for T {
    fn canonical_name(&self, name: String) -> Option<String> {
        match name.chars().next() {
            Some(x) => {
                if self.is_delimited_identifier_start(x) {
                    let mut chars = name.chars();
                    chars.next();
                    chars.next_back();
                    Some(chars.as_str().to_string())
                } else {
                    Some(name)
                }
            }
            _ => None,
        }
    }
    fn as_base(&self) -> &dyn Dialect {
        self
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
struct Context {
    // Set of aliases we discovered in this query. We don't want to return alias as input and
    // output, because they have no sense in outside of query they were defined
    aliases: HashSet<DbTableMeta>,
    // Tables used as input to this query. "Input" is defined liberally, query does not have
    // to read data to be treated as input - it's sufficient that it's referenced in a query somehow
    inputs: HashSet<DbTableMeta>,
    // Tables used as output to this query. Same as input, they have to be referenced - data does
    // not have to be actually written as a result of execution.
    outputs: HashSet<DbTableMeta>,
    // Some databases allow to specify default schema. When schema for table is not referenced,
    // we're using this default as it.
    default_schema: Option<String>,
    // Dialect used in this statements.
    dialect: Arc<dyn CanonicalDialect>,
}

impl Context {
    fn default() -> Context {
        Context {
            aliases: HashSet::new(),
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            default_schema: None,
            dialect: Arc::new(SnowflakeDialect),
        }
    }

    fn new(dialect: Arc<dyn CanonicalDialect>, default_schema: Option<&str>) -> Context {
        Context {
            aliases: HashSet::new(),
            inputs: HashSet::new(),
            outputs: HashSet::new(),
            default_schema: default_schema.map(String::from),
            dialect,
        }
    }

    fn add_table_alias(&mut self, alias: &TableAlias) {
        let name = DbTableMeta::new(alias.name.value.clone(), self);
        self.aliases.insert(name);
    }

    fn add_ident_alias(&mut self, alias: &Ident) {
        let name = DbTableMeta::new(alias.value.clone(), self);
        self.aliases.insert(name);
    }

    fn add_input(&mut self, table: &String) {
        let name = DbTableMeta::new(table.clone(), self);
        if !self.aliases.contains(&name) {
            self.inputs.insert(name);
        }
    }

    fn add_output(&mut self, output: &String) {
        let name = DbTableMeta::new(output.clone(), self);
        if !self.aliases.contains(&name) {
            self.outputs.insert(name);
        }
    }
}

#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct DbTableMeta {
    #[pyo3(get)]
    database: Option<String>,
    #[pyo3(get)]
    schema: Option<String>,
    #[pyo3(get)]
    name: String,
    // ..columns
}

impl DbTableMeta {
    fn new(name: String, context: &mut Context) -> Self {
        let mut split = name
            .split('.')
            .map(|x| {
                context
                    .dialect
                    .canonical_name(String::from(x))
                    .unwrap_or(String::from(x))
            })
            .collect::<Vec<String>>();
        split.reverse();
        let table_name = match split.get(0) {
            Some(x) => x,
            None => &name,
        };
        DbTableMeta {
            database: split.get(2).cloned(),
            schema: split.get(1).cloned().or(context.default_schema.clone()),
            name: table_name.clone(),
        }
    }
}

#[pymethods]
impl DbTableMeta {
    #[new]
    pub fn py_new(name: String) -> Self {
        DbTableMeta::new(name, &mut Context::default())
    }

    #[getter(qualified_name)]
    pub fn qualified_name(&self) -> String {
        format!(
            "{}{}{}",
            self.database
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or("".to_string()),
            self.schema
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or("".to_string()),
            self.name
        )
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.qualified_name() == other.qualified_name()),
            CompareOp::Ne => Ok(self.qualified_name() != other.qualified_name()),
            _ => Err(PyTypeError::new_err(format!(
                "can't use operator {op:?} on DbTableMeta"
            ))),
        }
    }

    fn __repr__(&self) -> String {
        self.qualified_name()
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __hash__(&self) -> isize {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish() as isize
    }
}

#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlMeta {
    #[pyo3(get)]
    pub in_tables: Vec<DbTableMeta>,
    #[pyo3(get)]
    pub out_tables: Vec<DbTableMeta>,
}

#[pymethods]
impl SqlMeta {
    fn __repr__(&self) -> String {
        format!(
            "{{\"in_tables\": {:?}, \"out_tables\": {:?} }}",
            self.in_tables, self.out_tables
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl SqlMeta {
    fn new(inputs: Vec<DbTableMeta>, outputs: Vec<DbTableMeta>) -> Self {
        let mut inputs: Vec<DbTableMeta> = inputs.clone();
        let mut outputs: Vec<DbTableMeta> = outputs.clone();
        inputs.sort();
        outputs.sort();
        SqlMeta {
            in_tables: inputs,
            out_tables: outputs,
        }
    }
}

fn parse_with(with: &With, context: &mut Context) -> Result<(), String> {
    for cte in &with.cte_tables {
        context.add_table_alias(&cte.alias);
        parse_query(&cte.query, context)?;
    }
    Ok(())
}

fn parse_table_factor(table: &TableFactor, context: &mut Context) -> Result<(), String> {
    match table {
        TableFactor::Table { name, .. } => {
            context.add_input(&name.to_string());
            Ok(())
        }
        TableFactor::Derived {
            lateral: _,
            subquery,
            alias,
        } => {
            parse_query(subquery, context)?;
            if let Some(a) = alias {
                context.add_table_alias(a);
            }
            Ok(())
        }
        _ => Err(format!(
            "TableFactor other than table or subquery not implemented: {table}"
        )),
    }
}

fn get_table_name_from_table_factor(table: &TableFactor) -> Result<String, String> {
    if let TableFactor::Table { name, .. } = table {
        Ok(name.to_string())
    } else {
        Err(format!(
            "Name can be got only from simple table, got {table}"
        ))
    }
}

/// Process expression in case where we want to extract lineage (for eg. in subqueries)
/// This means most enum types are untouched, where in other contexts they'd be processed.
fn parse_expr(expr: &Expr, context: &mut Context) -> Result<(), String> {
    match expr {
        Expr::Subquery(query) => {
            parse_query(query, context)?;
        }
        Expr::InSubquery {
            expr: _,
            subquery,
            negated: _,
        } => {
            parse_query(subquery, context)?;
        }
        Expr::BinaryOp { left, op: _, right } => {
            parse_expr(left, context)?;
            parse_expr(right, context)?;
        }
        Expr::UnaryOp { op: _, expr } => {
            parse_expr(expr, context)?;
        }
        Expr::Case {
            operand: _,
            conditions,
            results: _,
            else_result: _,
        } => {
            for condition in conditions {
                parse_expr(condition, context)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_select(select: &Select, context: &mut Context) -> Result<(), String> {
    for projection in &select.projection {
        match projection {
            SelectItem::UnnamedExpr(expr) => {
                parse_expr(&expr, context)?;
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                parse_expr(&expr, context)?;
                context.add_ident_alias(&alias);
            }
            _ => {}
        }
    }

    if let Some(into) = &select.into {
        context.add_output(&into.name.to_string())
    }

    for table in &select.from {
        parse_table_factor(&table.relation, context)?;
        for join in &table.joins {
            parse_table_factor(&join.relation, context)?;
        }
    }
    Ok(())
}

fn parse_setexpr(setexpr: &SetExpr, context: &mut Context) -> Result<(), String> {
    match setexpr {
        SetExpr::Select(select) => parse_select(select, context)?,
        SetExpr::Values(_) => (),
        SetExpr::Insert(stmt) => parse_stmt(stmt, context)?,
        SetExpr::Query(q) => parse_query(q, context)?,
        SetExpr::SetOperation {
            op: _,
            all: _,
            left,
            right,
        } => {
            parse_setexpr(left, context)?;
            parse_setexpr(right, context)?;
        }
    };
    Ok(())
}

fn parse_query(query: &Query, context: &mut Context) -> Result<(), String> {
    match &query.with {
        Some(with) => parse_with(with, context)?,
        None => (),
    };

    parse_setexpr(&query.body, context)?;
    Ok(())
}

fn parse_stmt(stmt: &Statement, context: &mut Context) -> Result<(), String> {
    match stmt {
        Statement::Query(query) => {
            parse_query(query, context)?;
            Ok(())
        }
        Statement::Insert {
            table_name, source, ..
        } => {
            parse_query(source, context)?;
            context.add_output(&table_name.to_string());
            Ok(())
        }
        Statement::Merge { table, source, .. } => {
            let table_name = get_table_name_from_table_factor(table)?;
            context.add_output(&table_name);
            parse_table_factor(source, context)?;
            Ok(())
        }
        Statement::CreateTable {
            name, query, like, clone, ..
        } => {
            if let Some(boxed_query) = query {
                parse_query(boxed_query.as_ref(), context)?;
            }
            if let Some(like_table) = like {
                context.add_input(&like_table.to_string());
            }
            if let Some(clone) = clone {
                context.add_input(&clone.to_string());
            }

            context.add_output(&name.to_string());
            Ok(())
        }
        Statement::Update {
            table,
            assignments: _,
            from,
            selection,
        } => {
            let name = get_table_name_from_table_factor(&table.relation)?;
            context.add_output(&name);

            if let Some(src) = from {
                parse_table_factor(&src.relation, context)?;
                for join in &src.joins {
                    parse_table_factor(&join.relation, context)?;
                }
            }
            if let Some(expr) = selection {
                parse_expr(expr, context)?;
            }
            Ok(())
        }
        Statement::Delete {
            table_name,
            using,
            selection,
        } => {
            let table_name = get_table_name_from_table_factor(table_name)?;
            context.add_output(&table_name.to_string());

            if let Some(using) = using {
                parse_table_factor(using, context)?;
            }

            if let Some(expr) = selection {
                parse_expr(expr, context)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn get_dialect(name: &str) -> Arc<dyn CanonicalDialect> {
    match name {
        "bigquery" => Arc::new(BigQueryDialect),
        "snowflake" => Arc::new(SnowflakeDialect),
        "postgres" => Arc::new(PostgreSqlDialect {}),
        "postgresql" => Arc::new(PostgreSqlDialect {}),
        "hive" => Arc::new(HiveDialect {}),
        "mysql" => Arc::new(MySqlDialect {}),
        "mssql" => Arc::new(MsSqlDialect {}),
        "sqlite" => Arc::new(SQLiteDialect {}),
        "ansi" => Arc::new(AnsiDialect {}),
        _ => Arc::new(GenericDialect),
    }
}

pub fn get_generic_dialect(name: Option<&str>) -> Arc<dyn CanonicalDialect> {
    if let Some(d) = name {
        get_dialect(d)
    } else {
        Arc::new(GenericDialect)
    }
}

pub fn parse_multiple_statements(
    sql: Vec<&str>,
    dialect: Arc<dyn CanonicalDialect>,
    default_schema: Option<&str>,
) -> Result<SqlMeta, String> {
    let mut inputs: HashSet<DbTableMeta> = HashSet::new();
    let mut outputs: HashSet<DbTableMeta> = HashSet::new();

    for statement in sql {
        let ast = match Parser::parse_sql(dialect.as_base(), statement) {
            Ok(k) => k,
            Err(e) => return Err(e.to_string()),
        };

        if ast.is_empty() {
            return Err(String::from("Empty statement list"));
        }

        for stmt in ast {
            let mut context = Context::new(dialect.clone(), default_schema);
            parse_stmt(&stmt, &mut context)?;
            for input in context.inputs.iter() {
                inputs.insert(input.clone());
            }
            for output in context.outputs.iter() {
                outputs.insert(output.clone());
            }
        }
    }
    Ok(SqlMeta::new(
        inputs.into_iter().collect(),
        outputs.into_iter().collect(),
    ))
}

pub fn parse_sql(
    sql: &str,
    dialect: Arc<dyn CanonicalDialect>,
    default_schema: Option<&str>,
) -> Result<SqlMeta, String> {
    parse_multiple_statements(vec![sql], dialect, default_schema)
}

// Parses SQL.
#[pyfunction]
fn parse(sql: Vec<&str>, dialect: Option<&str>, default_schema: Option<&str>) -> PyResult<SqlMeta> {
    match parse_multiple_statements(sql, get_generic_dialect(dialect), default_schema) {
        Ok(ok) => Ok(ok),
        Err(err) => Err(PyRuntimeError::new_err(err)),
    }
}

#[pyfunction]
fn provider() -> String {
    "rust".to_string()
}

/// A Python module implemented in Rust.
#[pymodule]
fn openlineage_sql(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    m.add_function(wrap_pyfunction!(provider, m)?)?;
    m.add_class::<SqlMeta>()?;
    m.add_class::<DbTableMeta>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::DbTableMeta;

    #[test]
    fn compare_db_meta() {
        assert_ne!(
            DbTableMeta {
                database: None,
                schema: None,
                name: "discount".to_string()
            },
            DbTableMeta {
                database: None,
                schema: Some("public".to_string()),
                name: "discount".to_string()
            }
        );
    }
}
