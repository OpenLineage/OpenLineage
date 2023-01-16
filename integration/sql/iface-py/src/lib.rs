// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

extern crate openlineage_sql as rust_impl;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use pyo3::basic::CompareOp;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use rust_impl::{get_generic_dialect, parse_multiple_statements};

#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct DbTableMeta(rust_impl::DbTableMeta);

#[pymethods]
impl DbTableMeta {
    #[new]
    pub fn py_new(name: String) -> Self {
        DbTableMeta(rust_impl::DbTableMeta::new_default_dialect(name))
    }

    #[getter(qualified_name)]
    pub fn qualified_name(&self) -> String {
        self.0.qualified_name()
    }

    #[getter(database)]
    pub fn database(&self) -> Option<&str> {
        self.0.database.as_deref()
    }

    #[getter(schema)]
    pub fn schema(&self) -> Option<&str> {
        self.0.schema.as_deref()
    }

    #[getter(name)]
    pub fn name(&self) -> &str {
        &self.0.name
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ColumnMeta(rust_impl::ColumnMeta);

#[pymethods]
impl ColumnMeta {
    #[new]
    pub fn py_new(name: String, origin: Option<DbTableMeta>) -> Self {
        Self(rust_impl::ColumnMeta::new(name, origin.map(|x| x.0)))
    }

    #[getter(origin)]
    pub fn origin(&self) -> Option<DbTableMeta> {
        self.0.origin.clone().map(DbTableMeta)
    }

    #[getter(name)]
    pub fn name(&self) -> String {
        self.0.name.clone()
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.0.name == other.0.name && self.0.origin == other.0.origin),
            CompareOp::Ne => Ok(self.0.name != other.0.name || self.0.origin != other.0.origin),
            _ => Err(PyTypeError::new_err(format!(
                "can't use operator {op:?} on ColumnMeta"
            ))),
        }
    }
}

impl ColumnMeta {
    fn __hash__(&self) -> isize {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish() as isize
    }
}

#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ColumnLineage {
    pub descendant: ColumnMeta,
    pub lineage: Vec<ColumnMeta>,
}

impl ColumnLineage {
    fn new(lineage: rust_impl::ColumnLineage) -> Self {
        Self {
            descendant: ColumnMeta(lineage.descendant.clone()),
            lineage: lineage.lineage.iter().cloned().map(ColumnMeta).collect(),
        }
    }
}

#[pymethods]
impl ColumnLineage {
    #[new]
    pub fn py_new(descendant: ColumnMeta, lineage: Vec<ColumnMeta>) -> Self {
        Self {
            descendant,
            lineage,
        }
    }

    #[getter(descendant)]
    pub fn descendant(&self) -> ColumnMeta {
        self.descendant.clone()
    }

    #[getter(lineage)]
    pub fn lineage(&self) -> Vec<ColumnMeta> {
        self.lineage.clone()
    }

    fn __repr__(&self) -> String {
        fn column_meta(cm: ColumnMeta) -> String {
            format!(
                "origin: ({:?}), name ({})",
                cm.0.origin
                    .map(|x| x.qualified_name())
                    .unwrap_or("unknown".to_string()),
                cm.0.name
            )
        }
        format!(
            "{} - [{}]",
            column_meta(self.descendant.clone()),
            self.lineage
                .iter()
                .cloned()
                .map(column_meta)
                .collect::<Vec<String>>()
                .join(",")
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => {
                Ok(self.lineage == other.lineage && self.descendant == other.descendant)
            }
            CompareOp::Ne => {
                Ok(self.lineage != other.lineage || self.descendant != other.descendant)
            }
            _ => Err(PyTypeError::new_err(format!(
                "can't use operator {op:?} on ColumnLineage"
            ))),
        }
    }

    fn __hash__(&self) -> isize {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish() as isize
    }
}

#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExtractionError(rust_impl::ExtractionError);

#[pymethods]
impl ExtractionError {
    #[new]
    pub fn py_new(index: usize, message: String, origin_statement: String) -> Self {
        Self(rust_impl::ExtractionError {
            index,
            message,
            origin_statement,
        })
    }

    #[getter(index)]
    pub fn index(&self) -> usize {
        self.0.index
    }

    #[getter(message)]
    pub fn message(&self) -> String {
        self.0.message.clone()
    }

    #[getter(origin_statement)]
    pub fn origin_statement(&self) -> String {
        self.0.origin_statement.clone()
    }

    fn __repr__(&self) -> String {
        fn column_meta(cm: ColumnMeta) -> String {
            format!(
                "index: ({}), message ({}), origin_statement",
                cm.0.origin
                    .map(|x| x.qualified_name())
                    .unwrap_or("unknown".to_string()),
                cm.0.name
            )
        }
        format!(
            "index: ({}), message ({}), origin_statement ({})",
            self.0.index, self.0.message, self.0.origin_statement
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.0.index == other.0.index
                && self.0.message == other.0.message
                && self.0.origin_statement == other.0.origin_statement),
            CompareOp::Ne => Ok(self.0.index != other.0.index
                || self.0.message != other.0.message
                || self.0.origin_statement != other.0.origin_statement),
            _ => Err(PyTypeError::new_err(format!(
                "can't use operator {op:?} on ExtractionError"
            ))),
        }
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
    #[pyo3(get)]
    pub column_lineage: Vec<ColumnLineage>,
    #[pyo3(get)]
    pub errors: Vec<ExtractionError>,
}

impl SqlMeta {
    fn new(meta: rust_impl::SqlMeta) -> Self {
        let in_tables = meta
            .table_lineage
            .in_tables
            .iter()
            .cloned()
            .map(DbTableMeta)
            .collect();
        let out_tables = meta
            .table_lineage
            .out_tables
            .iter()
            .cloned()
            .map(DbTableMeta)
            .collect();
        let column_lineage = meta
            .column_lineage
            .iter()
            .cloned()
            .map(ColumnLineage::new)
            .collect();
        let errors = meta.errors.iter().cloned().map(ExtractionError).collect();
        Self {
            in_tables,
            out_tables,
            column_lineage,
            errors,
        }
    }
}

#[pymethods]
impl SqlMeta {
    fn __repr__(&self) -> String {
        format!(
            "{{\"in_tables\": {:?}, \"out_tables\": {:?} \"column_lineage\": {:?}}}",
            self.in_tables, self.out_tables, self.column_lineage
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

// Parses SQL.
#[pyfunction]
fn parse(sql: Vec<&str>, dialect: Option<&str>, default_schema: Option<String>) -> Result<SqlMeta> {
    parse_multiple_statements(sql, get_generic_dialect(dialect), default_schema).map(SqlMeta::new)
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
    m.add_class::<ColumnLineage>()?;
    m.add_class::<ColumnMeta>()?;
    m.add_class::<ExtractionError>()?;
    Ok(())
}

fn pytable(x: &str) -> DbTableMeta {
    return DbTableMeta(rust_impl::DbTableMeta::new_default_dialect(String::from(x)));
}

fn pycolumn(column: &str) -> ColumnMeta {
    return ColumnMeta(rust_impl::ColumnMeta::new(String::from(column), None));
}

fn pycolumn_with_origin(column: &str, table: &str) -> ColumnMeta {
    return ColumnMeta(rust_impl::ColumnMeta::new(
        String::from(column),
        Some(rust_impl::DbTableMeta::new_default_dialect(String::from(
            table,
        ))),
    ));
}

#[test]
fn test_python_conversion() {
    let x = parse(
        vec![
            "WITH cte1 AS (
                SELECT col1, col2
                FROM table1
                WHERE col1 = 'value1'
            ), cte2 AS (
                SELECT col3, col4
                FROM table2
                WHERE col2 = 'value2'
            )
            SELECT cte1.col1, cte2.col3
            FROM cte1
            JOIN cte2 ON cte1.col2 = cte2.col4",
        ],
        None,
        None,
    )
    .unwrap();
    let y: SqlMeta = SqlMeta {
        in_tables: vec![pytable("table1"), pytable("table2")],
        out_tables: vec![],
        column_lineage: vec![
            ColumnLineage {
                descendant: pycolumn("col1"),
                lineage: vec![pycolumn_with_origin("col1", "table1")],
            },
            ColumnLineage {
                descendant: pycolumn("col3"),
                lineage: vec![pycolumn_with_origin("col3", "table2")],
            },
        ],
        errors: vec![],
    };
    assert_eq!(x, y);
}
