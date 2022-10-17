// Copyright 2018-2022 contributors to the OpenLineage project
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
        format!(
            "{}{}{}",
            self.0
                .database
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or_else(|| "".to_string()),
            self.0
                .schema
                .as_ref()
                .map(|x| format!("{}.", x))
                .unwrap_or_else(|| "".to_string()),
            self.0.name
        )
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlMeta {
    #[pyo3(get)]
    pub in_tables: Vec<DbTableMeta>,
    #[pyo3(get)]
    pub out_tables: Vec<DbTableMeta>,
    sql_meta_impl: rust_impl::SqlMeta,
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
        Self {
            in_tables,
            out_tables,
            sql_meta_impl: meta,
        }
    }
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
    Ok(())
}
