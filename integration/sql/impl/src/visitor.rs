// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::context::Context;
use crate::lineage::*;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor, With,
};

pub trait Visit {
    fn visit(&self, context: &mut Context) -> Result<()>;
}

impl Visit for With {
    fn visit(&self, context: &mut Context) -> Result<()> {
        for cte in &self.cte_tables {
            context.add_alias(cte.alias.name.value.clone());
            cte.query.visit(context)?;
        }
        Ok(())
    }
}

impl Visit for TableFactor {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            TableFactor::Table { name, .. } => {
                context.add_input(name.to_string());
                Ok(())
            }
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
            } => {
                subquery.visit(context)?;
                if let Some(a) = alias {
                    context.add_alias(a.name.value.clone());
                }
                Ok(())
            }
            _ => Err(anyhow!(
                "TableFactor other than table or subquery not implemented: {self}"
            )),
        }
    }
}

/// Process expression in case where we want to extract lineage (for eg. in subqueries)
/// This means most enum types are untouched, where in other contexts they'd be processed.
impl Visit for Expr {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            Expr::Subquery(query) => {
                query.visit(context)?;
            }
            Expr::InSubquery {
                expr: _,
                subquery,
                negated: _,
            } => {
                subquery.visit(context)?;
            }
            Expr::BinaryOp { left, op: _, right } => {
                left.visit(context)?;
                right.visit(context)?;
            }
            Expr::UnaryOp { op: _, expr } => {
                expr.visit(context)?;
            }
            Expr::Case {
                operand: _,
                conditions,
                results: _,
                else_result: _,
            } => {
                for condition in conditions {
                    condition.visit(context)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl Visit for Select {
    fn visit(&self, context: &mut Context) -> Result<()> {
        for projection in &self.projection {
            match projection {
                SelectItem::UnnamedExpr(expr) => {
                    expr.visit(context)?;
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    expr.visit(context)?;
                    context.add_alias(alias.value.clone());
                }
                _ => {}
            }
        }

        if let Some(into) = &self.into {
            context.add_output(into.name.to_string())
        }

        for table in &self.from {
            table.relation.visit(context)?;
            for join in &table.joins {
                join.relation.visit(context)?;
            }
        }
        Ok(())
    }
}

impl Visit for SetExpr {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            SetExpr::Select(select) => select.visit(context),
            SetExpr::Values(_) => Ok(()),
            SetExpr::Insert(stmt) => stmt.visit(context),
            SetExpr::Query(q) => q.visit(context),
            SetExpr::SetOperation {
                op: _,
                all: _,
                left,
                right,
            } => {
                left.visit(context)?;
                right.visit(context)
            }
        }
    }
}

impl Visit for Query {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match &self.with {
            Some(with) => with.visit(context)?,
            None => (),
        };

        self.body.visit(context)?;
        Ok(())
    }
}

impl Visit for Statement {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            Statement::Query(query) => query.visit(context),
            Statement::Insert {
                table_name, source, ..
            } => {
                source.visit(context)?;
                context.add_output(table_name.to_string());
                Ok(())
            }
            Statement::Merge { table, source, .. } => {
                let table_name = get_table_name_from_table_factor(table)?;
                context.add_output(table_name);
                source.visit(context)?;
                Ok(())
            }
            Statement::CreateTable {
                name,
                query,
                like,
                clone,
                ..
            } => {
                if let Some(query) = query {
                    query.visit(context)?;
                }
                if let Some(like_table) = like {
                    context.add_input(like_table.to_string());
                }
                if let Some(clone) = clone {
                    context.add_input(clone.to_string());
                }

                context.add_output(name.to_string());
                Ok(())
            }
            Statement::Update {
                table,
                assignments: _,
                from,
                selection,
            } => {
                let name = get_table_name_from_table_factor(&table.relation)?;
                context.add_output(name);

                if let Some(src) = from {
                    src.relation.visit(context)?;
                    for join in &src.joins {
                        join.relation.visit(context)?;
                    }
                }
                if let Some(expr) = selection {
                    expr.visit(context)?;
                }
                Ok(())
            }
            Statement::Delete {
                table_name,
                using,
                selection,
            } => {
                let table_name = get_table_name_from_table_factor(table_name)?;
                context.add_output(table_name);

                if let Some(using) = using {
                    using.visit(context)?;
                }

                if let Some(expr) = selection {
                    expr.visit(context)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

// --- Utils ---

fn get_table_name_from_table_factor(table: &TableFactor) -> Result<String> {
    if let TableFactor::Table { name, .. } = table {
        Ok(name.to_string())
    } else {
        Err(anyhow!(
            "Name can be got only from simple table, got {table}"
        ))
    }
}
