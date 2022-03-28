mod bigquery;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

pub use bigquery::BigQueryDialect;
use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::impl_::pyfunction::wrap_pyfunction;
use pyo3::prelude::*;
use sqlparser::ast::{
    Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor, With,
};
use sqlparser::dialect::{Dialect, PostgreSqlDialect, SnowflakeDialect};
use sqlparser::parser::Parser;

pub trait CanonicalDialect: Dialect {
    fn canonical_name(&mut self, name: String) -> String;
    fn as_base(&self) -> &dyn Dialect;
}

impl<T: Dialect> CanonicalDialect for T {
    fn canonical_name(&mut self, name: String) -> String {
        if self.is_delimited_identifier_start(name.chars().next().unwrap()) {
            let mut chars = name.chars();
            chars.next();
            chars.next_back();
            chars.as_str().to_string()
        } else {
            name
        }
    }
    fn as_base(&self) -> &dyn Dialect {
        self
    }
}

#[derive(Debug)]
struct Context {
    aliases: HashSet<DbTableMeta>,
    inputs: HashSet<DbTableMeta>,
    output: Option<DbTableMeta>,
    default_schema: Option<String>,
    dialect: Box<dyn CanonicalDialect>,
}

impl Context {
    fn default() -> Context {
        Context {
            aliases: HashSet::new(),
            inputs: HashSet::new(),
            output: None,
            default_schema: None,
            dialect: Box::new(SnowflakeDialect),
        }
    }

    fn new(dialect: Box<dyn CanonicalDialect>, default_schema: Option<&str>) -> Context {
        Context {
            aliases: HashSet::new(),
            inputs: HashSet::new(),
            output: None,
            default_schema: default_schema.map(|x| String::from(x)),
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

    fn set_output(&mut self, output: &String) {
        let name = output.clone();
        self.output = Some(DbTableMeta::new(name, self));
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
            .split(".")
            .map(|x| context.dialect.canonical_name(String::from(x)))
            .collect::<Vec<String>>();
        split.reverse();
        DbTableMeta {
            database: split.get(2).cloned(),
            schema: split.get(1).cloned().or(context.default_schema.clone()),
            name: split.get(0).unwrap().clone(),
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
                .unwrap_or(String::from("")),
            self.schema
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or(String::from("")),
            self.name
        )
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.qualified_name() == other.qualified_name()),
            CompareOp::Ne => Ok(self.qualified_name() == other.qualified_name()),
            _ => Err(PyTypeError::new_err(format!(
                "can't use operator {op:?} on DbTableMeta"
            ))),
        }
    }

    fn __repr__(&self) -> String {
        self.qualified_name()
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

impl From<Context> for SqlMeta {
    fn from(ctx: Context) -> Self {
        let outputs: Vec<DbTableMeta> = if ctx.output.is_some() {
            vec![ctx.output.unwrap()]
        } else {
            vec![]
        };
        let mut inputs: Vec<DbTableMeta> = ctx.inputs.into_iter().collect();
        inputs.sort();
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

        Expr::Identifier(_) => {}
        Expr::CompoundIdentifier(_) => {}
        Expr::IsNull(_) => {}
        Expr::IsNotNull(_) => {}
        Expr::IsDistinctFrom(_, _) => {}
        Expr::IsNotDistinctFrom(_, _) => {}
        Expr::InList { .. } => {}
        Expr::InSubquery {
            expr: _,
            subquery,
            negated: _,
        } => {
            parse_query(subquery, context)?;
        }
        Expr::InUnnest { .. } => {}
        Expr::Between { .. } => {}
        Expr::BinaryOp { left, op: _, right } => {
            parse_expr(left, context)?;
            parse_expr(right, context)?;
        }
        Expr::UnaryOp { op: _, expr } => {
            parse_expr(expr, context)?;
        }
        Expr::Cast { .. } => {}
        Expr::TryCast { .. } => {}
        Expr::Extract { .. } => {}
        Expr::Substring { .. } => {}
        Expr::Trim { .. } => {}
        Expr::Collate { .. } => {}
        Expr::Nested(_) => {}
        Expr::Value(_) => {}
        Expr::TypedString { .. } => {}
        Expr::MapAccess { .. } => {}
        Expr::Function(_) => {}
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
        Expr::Exists(_) => {}
        Expr::ListAgg(_) => {}
        Expr::GroupingSets(_) => {}
        Expr::Cube(_) => {}
        Expr::Rollup(_) => {}
        Expr::Tuple(_) => {}
        Expr::ArrayIndex { .. } => {}
        Expr::Array(_) => {}
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
        context.set_output(&into.name.to_string())
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
        SetExpr::Select(select) => parse_select(&select, context)?,
        SetExpr::Values(_) => (),
        SetExpr::Insert(stmt) => parse_stmt(stmt, context)?,
        SetExpr::Query(q) => parse_query(q, context)?,
        SetExpr::SetOperation {
            op: _,
            all: _,
            left,
            right,
        } => {
            parse_setexpr(&left, context)?;
            parse_setexpr(&right, context)?;
        }
    };
    Ok(())
}

fn parse_query(query: &Query, context: &mut Context) -> Result<(), String> {
    match &query.with {
        Some(with) => parse_with(&with, context)?,
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
            or: _,
            table_name,
            columns: _,
            overwrite: _,
            source,
            partitioned: _,
            after_columns: _,
            table: _,
            on: _,
        } => {
            parse_query(source, context)?;
            context.set_output(&table_name.to_string());
            Ok(())
        }
        Statement::Merge {
            table,
            source,
            alias,
            on: _,
            clauses: _,
        } => {
            let table_name = get_table_name_from_table_factor(table)?;
            context.set_output(&table_name);
            parse_setexpr(source, context)?;

            if let Some(a) = alias {
                context.add_table_alias(a);
            }

            Ok(())
        }
        Statement::CreateTable {
            or_replace: _,
            temporary: _,
            external: _,
            if_not_exists: _,
            name,
            columns: _,
            constraints: _,
            hive_distribution: _,
            hive_formats: _,
            table_properties: _,
            with_options: _,
            file_format: _,
            location: _,
            query,
            without_rowid: _,
            like,
            engine: _,
            default_charset: _,
            collation: _,
        } => {
            if let Some(boxed_query) = query {
                parse_query(boxed_query.as_ref(), context)?;
            }
            if let Some(like_table) = like {
                context.add_input(&like_table.to_string());
            }
            context.set_output(&name.to_string());
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn parse_sql(
    sql: &str,
    dialect: Box<dyn CanonicalDialect>,
    default_schema: Option<&str>,
) -> Result<SqlMeta, String> {
    let ast = match Parser::parse_sql(dialect.as_base(), sql) {
        Ok(k) => k,
        Err(e) => return Err(e.to_string().to_owned()),
    };

    if ast.is_empty() {
        return Err(String::from("Empty statement list"));
    }

    let mut context = Context::new(dialect, default_schema);
    let stmt = ast.first();

    parse_stmt(stmt.unwrap(), &mut context)?;
    Ok(SqlMeta::from(context))
}

// Parses SQL.
#[pyfunction]
fn parse(sql: &str, dialect: Option<&str>, default_schema: Option<&str>) -> PyResult<SqlMeta> {
    let parser_dialect: Box<dyn CanonicalDialect> = if let Some(d) = dialect {
        match d {
            "bigquery" => Box::new(BigQueryDialect),
            "snowflake" => Box::new(SnowflakeDialect),
            "postgres" => Box::new(PostgreSqlDialect {}),
            _ => Box::new(PostgreSqlDialect {}),
        }
    } else {
        Box::new(PostgreSqlDialect {})
    };

    match parse_sql(sql, parser_dialect, default_schema) {
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
