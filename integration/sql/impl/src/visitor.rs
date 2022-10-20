// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::context::Context;
use crate::lineage::*;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor, With, TableWithJoins, Function, FunctionArg, FunctionArgExpr, WindowSpec, OrderByExpr,
};

pub trait Visit {
    fn visit(&self, context: &mut Context) -> Result<()>;
}

impl Visit for With {
    fn visit(&self, context: &mut Context) -> Result<()> {
        for cte in &self.cte_tables {
            context.add_table_alias(DbTableMeta::new_default_dialect("".to_string()), cte.alias.name.value.clone());
            cte.query.visit(context)?;
        }
        Ok(())
    }
}

impl Visit for TableFactor {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            TableFactor::Table { name, alias, .. } => {
                let table = DbTableMeta::new(name.to_string(), context.dialect(), context.default_schema().clone());
                if let Some(alias) = alias {
                    context.add_table_alias(table.clone(), alias.name.value.clone());
                }
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
                    context.add_table_alias(DbTableMeta::new_default_dialect("".to_string()), a.name.value.clone());
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
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(expr) = operand {
                    expr.visit(context)?;
                }

                for condition in conditions {
                    condition.visit(context)?;
                }

                for result in results {
                    result.visit(context)?;
                }

                if let Some(expr) = else_result {
                    expr.visit(context)?;
                }
            }
            Expr::Identifier(id) => {
                let context_set = context.column_context().is_some();
                if context_set {
                    let descendant = context.column_context().as_ref().unwrap().name.clone();
                    context.add_column_ancestors(
                        descendant,
                        vec![ColumnMeta::new(
                            id.value.clone(),
                            context.table_context().clone(),
                        )],
                    );
                }
            }
            Expr::CompoundIdentifier(ids) => {
                let context_set = context.column_context().is_some();
                if context_set {
                    let descendant = context.column_context().as_ref().unwrap().name.clone();
                    let ancestor = ids.last().unwrap().value.clone();
                    let prefix = ids.iter().take(ids.len() - 1).rev().map(|i| i.value.clone()).collect::<Vec<String>>().join(".");
                    let table = DbTableMeta::new(prefix, context.dialect(), context.default_schema().clone());
                    context.add_column_ancestors(
                        descendant,
                        vec![ColumnMeta::new(
                            ancestor,
                            Some(table),
                        )],
                    );
                }
            }
            Expr::Function(func) => func.visit(context)?,
            _ => {}
        }
        Ok(())
    }
}

impl Visit for Function {
    fn visit(&self, context: &mut Context) -> Result<()> {
        for arg in &self.args {
            arg.visit(context)?;
        }

        if let Some(spec) = &self.over {
            spec.visit(context)?;
        }

        Ok(())
    }
}

impl Visit for FunctionArg {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            FunctionArg::Named { name: _, arg } => arg.visit(context),
            FunctionArg::Unnamed(arg) => arg.visit(context),
        }
    }
}

impl Visit for FunctionArgExpr {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            FunctionArgExpr::Expr(expr) => expr.visit(context),
            _ => Ok(()),
        }
    }
}

impl Visit for WindowSpec {
    fn visit(&self, context: &mut Context) -> Result<()> {
        for expr in &self.partition_by {
            expr.visit(context)?;
        }

        for order in &self.order_by {
            order.expr.visit(context)?;
        }

        Ok(())
    }
}

impl Visit for Select {
    fn visit(&self, context: &mut Context) -> Result<()> {
        // If we're selecting from a single table, that table becomes the default
        if self.from.len() == 1 {
            let t = self.from.first().unwrap();
            if let TableFactor::Table { name, alias, .. } = &t.relation {
                let table = DbTableMeta::new(name.to_string(), context.dialect(), context.default_schema().clone());
                if let Some(alias) = alias {
                    context.add_table_alias(table.clone(), alias.name.value.clone());
                }
                context.set_table_context(Some(table));
            }
        }

        for table in &self.from {
            table.relation.visit(context)?;
            for join in &table.joins {
                join.relation.visit(context)?;
            }
        }

        for projection in &self.projection {
            match projection {
                SelectItem::UnnamedExpr(expr) => {
                    match expr {
                        Expr::Identifier(id) => context
                            .set_column_context(Some(ColumnMeta::new(id.value.clone(), None))),
                        Expr::CompoundIdentifier(ids) => context.set_column_context(Some(
                            ColumnMeta::new(ids.last().unwrap().value.clone(), None),
                        )),
                        _ => context.set_unnamed_column_context(),
                    };
                    expr.visit(context)?;
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    context.set_column_context(Some(ColumnMeta::new(alias.value.clone(), None)));
                    expr.visit(context)?;
                }
                _ => {}
            }
        }

        context.set_column_context(None);

        if let Some(into) = &self.into {
            context.add_output(into.name.to_string())
        }

        context.set_table_context(None);
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
