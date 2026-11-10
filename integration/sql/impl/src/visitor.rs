// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::context::{cte_dependency_key, Context};
use crate::lineage::*;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AccessExpr, AlterTableOperation, CopyIntoSnowflakeKind, CreateTableLikeKind, Expr, FromTable,
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments, Ident, Join,
    JoinConstraint, JoinOperator, MergeAction, MergeInsertKind, MergeUpdateKind,
    MultiTableInsertIntoClause, ObjectName, ObjectNamePart, Query, RenameTableNameKind, Select,
    SelectItem, SetExpr, Statement, Subscript, Table, TableFactor, TableFunctionArgs, TableObject,
    UpdateTableFromKind, Use, Value, ValueWithSpan, WindowSpec, WindowType, With,
};
use sqlparser::dialect::{
    DatabricksDialect, Dialect, MsSqlDialect, MySqlDialect, SQLiteDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

pub trait Visit {
    fn visit(&self, context: &mut Context) -> Result<()>;
}

impl Visit for With {
    fn visit(&self, context: &mut Context) -> Result<()> {
        let size = self.cte_tables.len();
        // Snowflake, SQL Server and SQLite let any CTE refer to itself, with or
        // without RECURSIVE.
        let dialect = context.dialect().as_base();
        let recursive = self.recursive
            || dialect.is::<SnowflakeDialect>()
            || dialect.is::<MsSqlDialect>()
            || dialect.is::<SQLiteDialect>();
        for i in 0..size {
            context.add_table_alias(
                DbTableMeta::new_default_dialect("".to_string()),
                vec![self.cte_tables[i].alias.name.clone()],
            );
            context.push_frame();
            self.cte_tables[i].query.visit(context)?;
            let frame = context.pop_frame();
            if let Some(mut f) = frame {
                // The CTE body's own WITH scope ends with the body:
                // resolve its registrations before they merge into the
                // enclosing query, where they would capture references
                // meant for real tables or enclosing CTEs.
                context.resolve_subquery_cte_dependencies(&mut f);
                let table = DbTableMeta::new(
                    vec![self.cte_tables[i].alias.name.clone()],
                    context.dialect(),
                    context.default_schema().clone(),
                    context.default_database().clone(),
                );
                context.collect_with_table(f, table);
            }

            // CTEs are registered under their stored-form key (see
            // cte_dependency_key), so a reference the dialect resolves
            // to this CTE - however it is quoted or cased - finds it.
            let cte_key = cte_dependency_key(
                &self.cte_tables[i].alias.name.value,
                self.cte_tables[i].alias.name.quote_style,
                context.dialect().as_base(),
            );

            if i == 0 {
                context.collect_lower_nested_dependencies(cte_key.clone());
            }

            context.adjust_cte_dependencies(cte_key, recursive);
        }
        Ok(())
    }
}

impl Visit for TableFactor {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                // Check if table name is provided using IDENTIFIER function,
                // use it if found and valid, otherwise keep the current name
                let effective_name = match resolve_effective_table_name(name, args, context) {
                    Some(table_name) => table_name,
                    None => return Ok(()), // Exit if couldn't parse identifier function
                };

                let table = DbTableMeta::new(
                    convert_to_idents(&effective_name),
                    context.dialect(),
                    context.default_schema().clone(),
                    context.default_database().clone(),
                );
                if let Some(alias) = alias {
                    context.add_table_alias(table, vec![alias.name.clone()]);
                }

                if !context.is_main() {
                    context.add_table_dependency(convert_to_idents(&effective_name));
                }

                // Only a bare, single-part reference can name a CTE, and
                // it is looked up under the CTE's canonical key - the
                // object's display form would carry quote delimiters and
                // the as-written case, which never match a registration.
                // A qualified reference keeps its display form here, which
                // no bare key can equal, so it stays a real table.
                let cte_key = match effective_name.0.as_slice() {
                    [ObjectNamePart::Identifier(part)] => cte_dependency_key(
                        &part.value,
                        part.quote_style,
                        context.dialect().as_base(),
                    ),
                    _ => effective_name.to_string(),
                };

                if context.is_main() && !context.has_alias(cte_key.clone()) {
                    context.add_input(convert_to_idents(&effective_name));
                }

                if context.is_main() && context.has_alias(cte_key.clone()) {
                    context.mark_table_as_used(cte_key);
                }

                Ok(())
            }
            TableFactor::Pivot { table, alias, .. } => {
                table.visit(context)?;
                if let Some(ident) = get_table_name_from_table_factor(table, &*context) {
                    if let Some(pivot_alias) = alias {
                        context.add_table_alias(
                            DbTableMeta::new(
                                ident.clone(),
                                context.dialect(),
                                context.default_schema().clone(),
                                context.default_database().clone(),
                            ),
                            vec![pivot_alias.clone().name],
                        );
                    }
                }
                Ok(())
            }
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
                sample: _,
            } => {
                context.push_frame();
                subquery.visit(context)?;
                let mut frame = context.pop_frame().unwrap();
                // The subquery's WITH scope ends with it: resolve its
                // CTE registrations before they merge into the enclosing
                // query, where they would capture references meant for
                // real tables or enclosing CTEs.
                context.resolve_subquery_cte_dependencies(&mut frame);

                if let Some(alias) = alias {
                    let table = DbTableMeta::new(
                        vec![alias.clone().name],
                        context.dialect(),
                        context.default_schema().clone(),
                        context.default_database().clone(),
                    );
                    context.collect_with_table(frame, table);
                } else {
                    context.collect(frame);
                }

                Ok(())
            }
            TableFactor::TableFunction { .. } => {
                // https://docs.snowflake.com/en/sql-reference/functions-table
                // We can skip them as we don't support extracting lineage from functions
                Ok(())
            }
            TableFactor::Function { .. } => {
                // https://github.com/apache/datafusion-sqlparser-rs/pull/1026/files#r1373705587
                // This variant provides distinct functionality from TableFunction but can be
                // treated the same here
                Ok(())
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                table_with_joins.relation.visit(context)?;
                for join in &table_with_joins.joins {
                    join.visit(context)?;
                }
                Ok(())
            }
            TableFactor::UNNEST { array_exprs, .. } => {
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
                // BigQuery's `UNNEST(...)` (e.g. UNNEST(GENERATE_ARRAY(1, 1000)) AS n) is a
                // row generator over an array expression, not a real table - so UNNEST itself
                // contributes no input. But the array expression can still reference real
                // tables (e.g. UNNEST(ARRAY(SELECT id FROM source_table))), so visit each one
                // rather than discarding them outright.
                for array_expr in array_exprs {
                    array_expr.visit(context)?;
                }
                Ok(())
            }
            TableFactor::Unpivot { table, alias, .. } => {
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator
                // Same bug class as UNNEST above and #1358: this variant had no arm and fell
                // through to the catch-all Err. Mirrors the Pivot arm above - the real table
                // lineage lives in the wrapped `table`.
                table.visit(context)?;
                if let Some(ident) = get_table_name_from_table_factor(table, &*context) {
                    if let Some(unpivot_alias) = alias {
                        context.add_table_alias(
                            DbTableMeta::new(
                                ident.clone(),
                                context.dialect(),
                                context.default_schema().clone(),
                                context.default_database().clone(),
                            ),
                            vec![unpivot_alias.clone().name],
                        );
                    }
                }
                Ok(())
            }
            TableFactor::UnpivotExpr { expression, .. } => expression.visit(context),
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
                visit_subquery(query, context)?;
            }
            Expr::InSubquery {
                expr: _,
                subquery,
                negated: _,
            } => {
                visit_subquery(subquery, context)?;
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
                else_result,
                ..
            } => {
                if let Some(expr) = operand {
                    expr.visit(context)?;
                }

                for case_when in conditions {
                    case_when.condition.visit(context)?;
                    case_when.result.visit(context)?;
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
                        ColumnMeta::new(descendant, None),
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
                    let table = DbTableMeta::new(
                        ids.iter().take(ids.len() - 1).cloned().collect(),
                        context.dialect(),
                        context.default_schema().clone(),
                        context.default_database().clone(),
                    );
                    context.add_column_ancestors(
                        ColumnMeta::new(descendant, None),
                        vec![ColumnMeta::new(ancestor, Some(table))],
                    );
                }
            }
            Expr::Function(func) => func.visit(context)?,
            Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsJson { expr, .. } => {
                expr.visit(context)?;
            }
            Expr::AnyOp {
                left,
                compare_op: _,
                right,
                ..
            }
            | Expr::AllOp {
                left,
                compare_op: _,
                right,
            } => {
                left.visit(context)?;
                right.visit(context)?;
            }
            Expr::InList { expr, list, .. } => {
                expr.visit(context)?;
                for e in list {
                    e.visit(context)?;
                }
            }
            Expr::Between {
                expr,
                negated: _,
                low,
                high,
            } => {
                expr.visit(context)?;
                low.visit(context)?;
                high.visit(context)?;
            }
            Expr::Like {
                negated: _,
                expr,
                pattern,
                escape_char,
                ..
            }
            | Expr::ILike {
                negated: _,
                expr,
                pattern,
                escape_char,
                ..
            }
            | Expr::SimilarTo {
                negated: _,
                expr,
                pattern,
                escape_char,
                ..
            } => {
                expr.visit(context)?;
                pattern.visit(context)?;
                if let Some(escape) = escape_char {
                    escape.visit(context)?;
                }
            }
            Expr::Cast { expr, .. } => {
                expr.visit(context)?;
            }
            Expr::AtTimeZone { timestamp, .. } => {
                timestamp.visit(context)?;
            }
            Expr::Extract {
                field: _,
                syntax: _,
                expr,
            } => {
                expr.visit(context)?;
            }
            Expr::Position { expr, r#in } => {
                expr.visit(context)?;
                r#in.visit(context)?;
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                expr.visit(context)?;
                if let Some(e) = substring_from {
                    e.visit(context)?;
                }
                if let Some(e) = substring_for {
                    e.visit(context)?;
                }
            }
            Expr::Trim {
                expr,
                trim_where: _,
                trim_what,
                ..
            } => {
                expr.visit(context)?;
                if let Some(e) = trim_what {
                    e.visit(context)?;
                }
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                expr.visit(context)?;
                overlay_what.visit(context)?;
                overlay_from.visit(context)?;
                if let Some(e) = overlay_for {
                    e.visit(context)?;
                }
            }
            Expr::Collate { expr, .. } => {
                expr.visit(context)?;
            }
            Expr::Nested(expr) => {
                expr.visit(context)?;
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                root.visit(context)?;
                for access_expr in access_chain {
                    match access_expr {
                        AccessExpr::Dot(expr) => expr.visit(context)?,
                        AccessExpr::Subscript(subscript) => match subscript {
                            Subscript::Index { index } => index.visit(context)?,
                            Subscript::Slice {
                                lower_bound,
                                upper_bound,
                                stride,
                            } => {
                                if let Some(lower) = lower_bound {
                                    lower.visit(context)?;
                                }
                                if let Some(upper) = upper_bound {
                                    upper.visit(context)?;
                                }
                                if let Some(stride_expr) = stride {
                                    stride_expr.visit(context)?;
                                }
                            }
                        },
                    }
                }
            }
            Expr::Exists { subquery, .. } => {
                visit_subquery(subquery, context)?;
            }
            Expr::GroupingSets(list) | Expr::Cube(list) | Expr::Rollup(list) => {
                for exprs in list {
                    for expr in exprs {
                        expr.visit(context)?;
                    }
                }
            }
            Expr::Tuple(exprs) => {
                for expr in exprs {
                    expr.visit(context)?;
                }
            }
            Expr::Array(array) => {
                for e in &array.elem {
                    e.visit(context)?;
                }
            }
            Expr::JsonAccess { value, path: _ } => {
                value.visit(context)?;
            }
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                left.visit(context)?;
                right.visit(context)?;
            }
            Expr::InUnnest {
                expr, array_expr, ..
            } => {
                expr.visit(context)?;
                array_expr.visit(context)?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl Visit for Function {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match &self.args {
            FunctionArguments::None => {}
            FunctionArguments::Subquery(query) => visit_subquery(query, context)?,
            FunctionArguments::List(arguments) => {
                for arg in &arguments.args {
                    arg.visit(context)?;
                }
                for clause in &arguments.clauses {
                    if let FunctionArgumentClause::Where(predicate) = clause {
                        predicate.visit(context)?;
                    }
                }
            }
        }
        if let Some(filter) = &self.filter {
            filter.visit(context)?;
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
            FunctionArg::Named {
                name: _,
                arg,
                operator: _,
            }
            | FunctionArg::ExprNamed {
                name: _,
                arg,
                operator: _,
            } => arg.visit(context),
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

impl Visit for WindowType {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            WindowType::WindowSpec(spec) => spec.visit(context),
            WindowType::NamedWindow(..) => Ok(()),
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

impl Visit for Join {
    fn visit(&self, context: &mut Context) -> Result<()> {
        self.relation.visit(context)?;

        let constraint = match &self.join_operator {
            JoinOperator::Join(constraint)
            | JoinOperator::Inner(constraint)
            | JoinOperator::Left(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::Right(constraint)
            | JoinOperator::RightOuter(constraint)
            | JoinOperator::FullOuter(constraint)
            | JoinOperator::CrossJoin(constraint)
            | JoinOperator::Semi(constraint)
            | JoinOperator::LeftSemi(constraint)
            | JoinOperator::RightSemi(constraint)
            | JoinOperator::Anti(constraint)
            | JoinOperator::LeftAnti(constraint)
            | JoinOperator::RightAnti(constraint)
            | JoinOperator::StraightJoin(constraint) => constraint,
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => {
                match_condition.visit(context)?;
                constraint
            }
            JoinOperator::CrossApply
            | JoinOperator::OuterApply
            | JoinOperator::ArrayJoin
            | JoinOperator::LeftArrayJoin
            | JoinOperator::InnerArrayJoin => return Ok(()),
        };

        if let JoinConstraint::On(expr) = constraint {
            expr.visit(context)?;
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
                let table = DbTableMeta::new(
                    convert_to_idents(name),
                    context.dialect(),
                    context.default_schema().clone(),
                    context.default_database().clone(),
                );
                if let Some(alias) = alias {
                    context.add_table_alias(table.clone(), vec![alias.clone().name]);
                }
                context.set_table_context(Some(table));
            }
        }

        context.push_frame();

        for table in &self.from {
            context.push_frame();
            table.relation.visit(context)?;
            let frame = context.pop_frame().unwrap();
            context.collect_aliases(&frame);
            context.collect(frame);

            for join in &table.joins {
                context.push_frame();
                join.visit(context)?;
                let frame = context.pop_frame().unwrap();
                context.collect_aliases(&frame);
                context.collect(frame);
            }
        }

        let tables_frame = context.pop_frame().unwrap();
        context.collect_aliases(&tables_frame);

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

        if let Some(selection) = &self.selection {
            context.push_frame();
            selection.visit(context)?;
            let frame = context.pop_frame().unwrap();
            context.collect_aliases(&frame);
        }

        if let Some(having) = &self.having {
            context.push_frame();
            having.visit(context)?;
            let frame = context.pop_frame().unwrap();
            context.collect_aliases(&frame);
        }

        if let Some(qualify) = &self.qualify {
            context.push_frame();
            qualify.visit(context)?;
            let frame = context.pop_frame().unwrap();
            context.collect_aliases(&frame);
        }

        if let Some(into) = &self.into {
            // MySQL SELECT INTO assigns variables rather than creating tables.
            if !context.dialect().as_base().is::<MySqlDialect>() {
                for target in &into.targets {
                    match target {
                        Expr::Identifier(ident)
                            if ident.quote_style.is_none() && ident.value.starts_with('@') => {}
                        Expr::Identifier(ident) => context.add_output(vec![ident.clone()]),
                        Expr::CompoundIdentifier(idents) => context.add_output(idents.clone()),
                        Expr::Value(ValueWithSpan {
                            value: Value::Placeholder(_),
                            ..
                        }) => {}
                        _ => return Err(anyhow!("SELECT INTO target not implemented: {target}")),
                    }
                }
            }
        }

        context.set_table_context(None);

        context.coalesce(tables_frame);

        Ok(())
    }
}

impl Visit for SetExpr {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            SetExpr::Select(select) => select.visit(context),
            SetExpr::Values(values) => {
                for row in &values.rows {
                    for expression in row.iter() {
                        expression.visit(context)?;
                    }
                }
                Ok(())
            }
            SetExpr::Insert(stmt) => stmt.visit(context),
            // A parenthesized query is a scope of its own - as a set
            // operation branch it must not leave its CTE registrations
            // in the shared set-operation frame, where they would
            // capture the other branches' references.
            SetExpr::Query(q) => visit_subquery(q, context),
            SetExpr::SetOperation {
                op: _,
                set_quantifier: _,
                left,
                right,
            } => {
                left.visit(context)?;
                right.visit(context)
            }
            SetExpr::Table(table) => table.visit(context),
            SetExpr::Update(stmt) => stmt.visit(context),
            SetExpr::Delete(stmt) => stmt.visit(context),
            SetExpr::Merge(stmt) => stmt.visit(context),
        }
    }
}

impl Visit for Query {
    fn visit(&self, context: &mut Context) -> Result<()> {
        if !self.pipe_operators.is_empty() {
            return Err(anyhow!("SQL pipe operators are not supported"));
        }

        context.push_frame();
        // Whether the region this query sits in is the main body -
        // the body frame below may only be promoted back to main if
        // it is. A query nested inside a CTE body keeps the enclosing
        // region's flag: its direct table reads are dependencies of
        // that CTE, not inputs of the statement.
        let enclosing_is_main = context.is_main();
        if self.with.is_some() {
            context.unset_frame_to_main_body();
        }

        if let Some(with) = &self.with {
            with.visit(context)?
        }
        let with_frame = context.pop_frame().unwrap();

        context.collect_aliases(&with_frame);

        context.push_frame();
        if self.with.is_some() && enclosing_is_main {
            context.set_frame_to_main_body();
        }

        self.body.visit(context)?;

        if self.with.is_some() {
            context.unset_frame_to_main_body();
        }

        let frame = context.pop_frame().unwrap();
        context.collect(frame);

        // Resolve CTEs
        context.coalesce(with_frame);

        Ok(())
    }
}

impl Visit for Statement {
    fn visit(&self, context: &mut Context) -> Result<()> {
        context.push_frame();
        match self {
            Statement::Query(query) => query.visit(context)?,
            Statement::Insert(insert) => {
                if let Some(src) = &insert.source {
                    src.visit(context)?;
                }
                // Multi-table inserts (Snowflake/Oracle INSERT ALL and
                // INSERT FIRST) carry their targets in the multi-table
                // clauses, and `insert.table` is empty for them. Collect
                // every clause target as an output.
                for clause in &insert.multi_table_into_clauses {
                    add_multi_table_target(clause, context);
                }
                for when_clause in &insert.multi_table_when_clauses {
                    // A WHEN predicate is a general expression and can
                    // read tables through subqueries; visit it so those
                    // tables join the statement's inputs. Like a SELECT
                    // predicate, it is visited in a temporary frame: the
                    // columns a predicate subquery projects are compared
                    // or tested, never inserted, so they must not enter
                    // the INSERT's column lineage. Only the frame's table
                    // dependencies are kept: a CTE or alias defined inside
                    // one predicate is scoped to it, and registering it on
                    // the statement frame would hide a real table of the
                    // same name read by a later predicate.
                    context.push_frame();
                    when_clause.condition.visit(context)?;
                    let frame = context.pop_frame().unwrap();
                    context.collect_dependencies(&frame);
                    for clause in &when_clause.into_clauses {
                        add_multi_table_target(clause, context);
                    }
                }
                if let Some(else_clauses) = &insert.multi_table_else_clause {
                    for clause in else_clauses {
                        add_multi_table_target(clause, context);
                    }
                }
                match &insert.table {
                    TableObject::TableName(name) => {
                        if let Some(name) =
                            resolve_insert_target_name(name, &insert.columns, context)
                        {
                            let idents = convert_to_idents(&name);
                            // An empty name can never be a real target (it is
                            // what multi-table inserts leave behind); registering
                            // it would panic in DbTableMeta construction.
                            if !idents.is_empty() {
                                context.add_insert_output(idents);
                            }
                        }
                    }
                    TableObject::TableFunction(func) => {
                        func.visit(context)?;
                    }
                    TableObject::TableQuery(_) => {}
                }
            }
            Statement::Merge(merge) => {
                if let Some(table_name) = get_table_name_from_table_factor(&merge.table, &*context)
                {
                    context.add_output(table_name);
                }
                merge.source.visit(context)?;
                merge.on.visit(context)?;
                // A WHEN clause's predicate and action expressions are
                // general expressions, like the ON clause: they can read
                // tables through subqueries, and those tables are inputs
                // of the statement. Visit each clause in a temporary
                // frame, as a multi-table INSERT visits its WHEN
                // predicates, and keep only the frame's table
                // dependencies: a CTE or alias defined inside one clause
                // is scoped to it and must not capture a later clause's
                // references.
                for clause in &merge.clauses {
                    context.push_frame();
                    if let Some(predicate) = &clause.predicate {
                        predicate.visit(context)?;
                    }
                    match &clause.action {
                        MergeAction::Insert(insert) => {
                            if let MergeInsertKind::Values(values) = &insert.kind {
                                for row in &values.rows {
                                    for expr in &row.content {
                                        expr.visit(context)?;
                                    }
                                }
                            }
                            if let Some(predicate) = &insert.insert_predicate {
                                predicate.visit(context)?;
                            }
                        }
                        MergeAction::Update(update) => {
                            if let MergeUpdateKind::Set(assignments) = &update.kind {
                                for assignment in assignments {
                                    assignment.value.visit(context)?;
                                }
                            }
                            if let Some(predicate) = &update.update_predicate {
                                predicate.visit(context)?;
                            }
                            if let Some(predicate) = &update.delete_predicate {
                                predicate.visit(context)?;
                            }
                        }
                        MergeAction::Delete { .. } | MergeAction::DoNothing { .. } => {}
                    }
                    let frame = context.pop_frame().unwrap();
                    context.collect_dependencies(&frame);
                }
            }
            Statement::CreateTable(ct) => {
                if let Some(query) = &ct.query {
                    query.visit(context)?;
                }
                if let Some(like_table) = &ct.like {
                    match like_table {
                        CreateTableLikeKind::Parenthesized(ctl)
                        | CreateTableLikeKind::Plain(ctl) => {
                            context.add_input(convert_to_idents(&ctl.name))
                        }
                    };
                }
                if let Some(clone) = &ct.clone {
                    context.add_input(convert_to_idents(clone));
                }

                context.add_output(convert_to_idents(&ct.name));
            }
            Statement::CreateView(cv) => {
                cv.query.visit(context)?;
                context.add_output(convert_to_idents(&cv.name));
            }
            Statement::CreateStage {
                name, stage_params, ..
            } => {
                if stage_params.url.as_ref().is_some() {
                    context.add_non_table_input(
                        vec![Ident::new(stage_params.url.as_ref().unwrap().to_string())],
                        true,
                        true,
                    );
                }
                context.add_non_table_output(convert_to_idents(name), false, true);
            }
            Statement::Update(update) => {
                if let Some(name) =
                    get_table_name_from_table_factor(&update.table.relation, &*context)
                {
                    context.add_output(name);
                }

                for join in &update.table.joins {
                    join.visit(context)?;
                }

                if let Some(from) = &update.from {
                    match from {
                        UpdateTableFromKind::BeforeSet(tables)
                        | UpdateTableFromKind::AfterSet(tables) => {
                            for table_with_joins in tables {
                                table_with_joins.relation.visit(context)?;
                                for join in &table_with_joins.joins {
                                    join.visit(context)?;
                                }
                            }
                        }
                    }
                }
                for assignment in &update.assignments {
                    assignment.value.visit(context)?;
                }
                if let Some(expr) = &update.selection {
                    expr.visit(context)?;
                }
            }
            Statement::AlterTable(alter) => {
                for operation in &alter.operations {
                    match operation {
                        AlterTableOperation::SwapWith { table_name } => {
                            // both table names are inputs and outputs of the swap operation
                            context.add_input(convert_to_idents(table_name));
                            context.add_input(convert_to_idents(&alter.name));

                            context.add_output(convert_to_idents(table_name));
                            context.add_output(convert_to_idents(&alter.name));
                        }
                        AlterTableOperation::RenameTable { table_name } => {
                            context.add_input(convert_to_idents(&alter.name));
                            match table_name {
                                RenameTableNameKind::As(new_name)
                                | RenameTableNameKind::To(new_name) => {
                                    context.add_output(convert_to_idents(new_name))
                                }
                            };
                        }
                        _ => context.add_output(convert_to_idents(&alter.name)),
                    }
                }
            }
            Statement::Delete(delete) => {
                let tables = match &delete.from {
                    FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                        tables
                    }
                };

                if delete.tables.is_empty() {
                    for table in tables {
                        if let Some(output) =
                            get_table_name_from_table_factor(&table.relation, &*context)
                        {
                            context.add_output(output);
                        }
                        for join in &table.joins {
                            if let Some(output) =
                                get_table_name_from_table_factor(&join.relation, &*context)
                            {
                                context.add_output(output);
                            }
                        }
                    }
                } else {
                    let existing_inputs = context.inputs.clone();
                    for table in tables {
                        table.relation.visit(context)?;
                        for join in &table.joins {
                            join.visit(context)?;
                        }
                    }
                    for table in &delete.tables {
                        context.move_input_to_output(convert_to_idents(table));
                    }
                    context.inputs.extend(existing_inputs);
                }

                if let Some(using) = &delete.using {
                    for table in using {
                        table.relation.visit(context)?;
                        for join in &table.joins {
                            join.visit(context)?;
                        }
                    }
                }

                if let Some(expr) = &delete.selection {
                    expr.visit(context)?;
                }
            }
            Statement::Truncate(truncate) => {
                for table in &truncate.table_names {
                    context.add_output(convert_to_idents(&table.name))
                }
            }
            Statement::Drop { names, .. } => {
                for name in names {
                    context.add_output(convert_to_idents(name))
                }
            }
            Statement::CopyIntoSnowflake {
                kind,
                into,
                from_obj,
                from_query,
                ..
            } => match kind {
                CopyIntoSnowflakeKind::Table => {
                    context.add_output(convert_to_idents(into));
                    if let Some(from_object) = from_obj {
                        context.add_non_table_input(
                            convert_copy_location_to_idents(from_object),
                            true,
                            true,
                        );
                    }
                }
                CopyIntoSnowflakeKind::Location => {
                    context.add_non_table_output(convert_copy_location_to_idents(into), true, true);
                    if let Some(from_object) = from_obj {
                        context.add_input(convert_to_idents(from_object));
                    }
                    if let Some(query) = from_query {
                        query.visit(context)?;
                    }
                }
            },
            Statement::Use(use_enum) => {
                // We expect either one id (USE [...] foo;) or two ids (USE [...] foo.bar;)
                // No arm of this match may return early: the frame pushed
                // for the statement is popped by the common tail below,
                // and returning here would leave it on the stack for the
                // rest of the script.
                let (first_id, second_id) = match use_enum {
                    Use::Catalog(object_name)
                    | Use::Schema(object_name)
                    | Use::Database(object_name)
                    | Use::Object(object_name) => extract_up_to_two_ident_values(object_name),
                    _ => (None, None),
                };

                // Two ids are present, set db and schema
                if first_id.is_some() && second_id.is_some() {
                    context.set_default_database(first_id.clone());
                    context.set_default_schema(second_id.clone());
                } else if first_id.is_some() {
                    // One id is present, each dialect can work differently
                    match use_enum {
                        Use::Catalog(_) => {
                            context.set_default_database(first_id); // Databricks specific
                        }
                        Use::Schema(_) => {
                            context.set_default_schema(first_id.clone()); // Snowflake specific
                        }
                        Use::Database(_) => {
                            if context.dialect().as_base().is::<DatabricksDialect>() {
                                // Databricks treats DATABASE as alias for SCHEMA
                                context.set_default_schema(first_id);
                            } else {
                                context.set_default_database(first_id); // Snowflake specific
                            }
                        }
                        Use::Object(_) => {
                            if context.dialect().as_base().is::<DatabricksDialect>() {
                                // Databricks treats single id with no keyword as SCHEMA
                                context.set_default_schema(first_id);
                            } else {
                                context.set_default_database(first_id);
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        let mut frame = context.pop_frame().unwrap();
        if matches!(self, Statement::Insert(_)) {
            // An INSERT can sit inside a statement-level WITH (the
            // query body is a SetExpr::Insert), with its source
            // query's own CTE registrations accumulated on this
            // statement frame. collect_inputs below inserts a used
            // registration's dependencies verbatim, so a bare
            // dependency naming a CTE of the enclosing statement
            // would be emitted as a table and the CTE's own inputs
            // lost. Resolve this frame the way a subquery's frame is
            // resolved: such a dependency marks the enclosing CTE
            // used instead.
            context.resolve_subquery_cte_dependencies(&mut frame);
        }
        let cte_deps = frame.cte_dependencies.clone();
        let deps = frame.dependencies.clone();
        let bare_deps = frame.bare_dependencies.clone();
        context.collect_inputs(cte_deps, deps, bare_deps);
        context.collect(frame);
        context.clear_global_cte_dependencies();

        Ok(())
    }
}

impl Visit for Table {
    fn visit(&self, context: &mut Context) -> Result<()> {
        if let Some(name) = &self.table_name {
            context.add_input(vec![Ident::new(name.to_string())])
        }
        Ok(())
    }
}

// --- Utils ---

/// Visit a subquery appearing inside an expression. The subquery runs
/// in a dedicated frame: when it returns, its CTE registrations are
/// resolved and dropped (see
/// [`Context::resolve_subquery_cte_dependencies`]) before the frame's
/// outcome merges into the enclosing frame, so a CTE defined inside
/// the subquery stays invisible outside it.
fn visit_subquery(query: &Query, context: &mut Context) -> Result<()> {
    context.push_frame();
    let result = query.visit(context);
    let mut frame = context.pop_frame().unwrap();
    context.resolve_subquery_cte_dependencies(&mut frame);
    // Deliberately no collect_aliases here (TableFactor::Derived does
    // the same): the frame's aliases are scoped to the subquery, and
    // the CTE names among them - each registered as an alias of an
    // empty table - would otherwise leak into the enclosing frame and
    // hide real tables of the same name read later. collect() already
    // merges everything that may travel up: column ancestry,
    // dependencies and the (resolved) CTE outcome.
    context.collect(frame);
    result
}

/// Register one multi-table insert clause target (an INTO clause of a
/// Snowflake/Oracle INSERT ALL or INSERT FIRST) as an output. The
/// target's effective name is resolved first: IDENTIFIER('t') names the
/// table through a function call, and converting the raw name would
/// register a false output called IDENTIFIER and lose the real target.
/// The unconditional into-clauses, the WHEN clauses' into-clauses and
/// the ELSE into-clauses all go through here so the three paths cannot
/// drift apart.
fn add_multi_table_target(clause: &MultiTableInsertIntoClause, context: &mut Context) {
    if let Some(name) = resolve_effective_table_name(&clause.table_name, &None, context) {
        // The same guard as the regular INSERT target: a quoted empty
        // name (INTO "") names nothing and must not become an output.
        if !is_empty_object_name(&name) {
            context.add_insert_output(convert_to_idents(&name));
        }
    }
}

/// Resolve the effective name of a regular INSERT target. Databricks and
/// SQL Server also accept IDENTIFIER('db.schema.t') as the target, but
/// sqlparser parses that as the bare name IDENTIFIER with the string
/// landing in the statement's column list, so recover the real name from
/// there when the shape matches exactly: a single-part `identifier` name
/// and one single-quoted pseudo-column. A real table named `identifier`
/// with a genuine column list has unquoted columns and is unaffected.
fn resolve_insert_target_name(
    name: &ObjectName,
    columns: &[ObjectName],
    context: &Context,
) -> Option<ObjectName> {
    let is_mssql_or_databricks = context.dialect().as_base().is::<DatabricksDialect>()
        || context.dialect().as_base().is::<MsSqlDialect>();
    let is_bare_identifier = is_mssql_or_databricks
        && name.0.len() == 1
        && matches!(
            name.0.first(),
            Some(ObjectNamePart::Identifier(part)) if part.value.eq_ignore_ascii_case("identifier")
        );
    if is_bare_identifier && !columns.is_empty() {
        // The target carries something in its column list. For an
        // IDENTIFIER(...) target that is the call's argument, spilled
        // here by sqlparser; a single-quoted string is the one
        // argument form that resolves to a table name.
        let pseudo_column = match columns {
            [column] if column.0.len() == 1 => match column.0.first() {
                Some(ObjectNamePart::Identifier(part)) if part.quote_style == Some('\'') => {
                    Some(part.value.clone())
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(value) = pseudo_column {
            let resolved = parse_identifier_string(&value, context.dialect().as_base());
            // The same check the function form applies: an empty or
            // blank string names nothing.
            return if is_empty_object_name(&resolved) {
                None
            } else {
                Some(resolved)
            };
        }
        // Any other argument form - quoted parts, a variable, several
        // values - cannot be resolved, and then the target is an
        // IDENTIFIER(...) call, not a table called `identifier`: emit
        // no output rather than a phantom IDENTIFIER one. A list of
        // plain column names, on the other hand, belongs to a genuine
        // table called `identifier` and falls through.
        if !columns.iter().all(is_plain_column_name) {
            return None;
        }
    }
    match resolve_effective_table_name(name, &None, context) {
        // A quoted empty identifier (INSERT INTO "") names nothing,
        // like the empty IDENTIFIER('') string above: emit no output
        // rather than one with an empty name.
        Some(resolved) if is_empty_object_name(&resolved) => None,
        resolved => resolved,
    }
}

/// Whether an insert column is a plain column name, as opposed to an
/// IDENTIFIER(...) argument that sqlparser spilled into the column
/// list: one unquoted, bracket-quoted or backtick-quoted identifier.
fn is_plain_column_name(column: &ObjectName) -> bool {
    column.0.len() == 1
        && matches!(
            column.0.first(),
            Some(ObjectNamePart::Identifier(part))
                if matches!(part.quote_style, None | Some('[') | Some('`'))
        )
}

fn get_table_name_from_table_factor(table: &TableFactor, context: &Context) -> Option<Vec<Ident>> {
    if let TableFactor::Table { name, args, .. } = table {
        resolve_effective_table_name(name, args, context)
            .map(|table_name| convert_to_idents(&table_name))
    } else {
        None
    }
}

fn resolve_effective_table_name(
    name: &ObjectName,
    args: &Option<TableFunctionArgs>,
    context: &Context,
) -> Option<ObjectName> {
    if context.dialect().as_base().is::<DatabricksDialect>()
        || context.dialect().as_base().is::<MsSqlDialect>()
    {
        let is_identifier_function = name
            .0
            .first()
            .map(|part| match part {
                ObjectNamePart::Function(_) => false,
                ObjectNamePart::Identifier(ident) => ident.value.to_lowercase() == "identifier",
            })
            .unwrap_or(false);

        if !is_identifier_function {
            return Some(name.clone());
        }
        // Only a genuine IDENTIFIER() call carries arguments. With none,
        // this is a plain, possibly qualified, table name whose first
        // part happens to be `identifier` - a schema or catalog named
        // `identifier`, say - and it must be kept as written.
        let args = match args {
            Some(args) => args,
            None => return Some(name.clone()),
        };
        if args.args.len() != 1 {
            return None; // Return None if the length is not 1, we do not support it yet
        }
        get_table_name_from_identifier_function_args(&args.args, context.dialect().as_base())
    } else if context.dialect().as_base().is::<SnowflakeDialect>() {
        name.0.first().and_then(|part| match part {
            ObjectNamePart::Function(f) => {
                if f.name.value.to_lowercase() == "identifier" {
                    get_table_name_from_identifier_function_args(
                        &f.args,
                        context.dialect().as_base(),
                    )
                } else {
                    Some(name.clone())
                }
            }
            ObjectNamePart::Identifier(_) => Some(name.clone()),
        })
    } else {
        Some(name.clone())
    }
}

/// Whether an object name is a single identifier that is empty or
/// blank - what IDENTIFIER('') and IDENTIFIER('   ') resolve to.
/// Such a name names nothing.
fn is_empty_object_name(name: &ObjectName) -> bool {
    name.0.len() == 1
        && matches!(
            name.0.first(),
            Some(ObjectNamePart::Identifier(part)) if part.value.trim().is_empty()
        )
}

fn get_table_name_from_identifier_function_args(
    args: &[FunctionArg],
    dialect: &dyn Dialect,
) -> Option<ObjectName> {
    let arg = args.first()?; // Return None if empty

    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
            Expr::Identifier(ident) => Some(ObjectName::from(vec![ident.clone()])),
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(ident),
                span: _,
            }) => {
                let name = parse_identifier_string(ident, dialect);
                // IDENTIFIER('') names nothing: emit no dataset rather
                // than one with an empty name.
                if is_empty_object_name(&name) {
                    None
                } else {
                    Some(name)
                }
            }
            _ => None,
        },
        _ => None,
    }
}

/// The string argument of IDENTIFIER(...) is itself an object name:
/// IDENTIFIER('db.schema.t1') refers to table t1 in db.schema, not to a
/// table literally named "db.schema.t1". Parse it with the statement's
/// dialect so quoted components keep their quoting and case. A string that
/// does not parse as one complete object name keeps the historical
/// treatment of the whole string as a single identifier.
fn parse_identifier_string(value: &str, dialect: &dyn Dialect) -> ObjectName {
    let parsed = (|| {
        let mut parser = Parser::new(dialect).try_with_sql(value).ok()?;
        let name = parser.parse_object_name(false).ok()?;
        let fully_consumed = parser.peek_token().token == Token::EOF;
        let all_identifiers = name
            .0
            .iter()
            .all(|part| matches!(part, ObjectNamePart::Identifier(_)));
        if fully_consumed && all_identifiers && !name.0.is_empty() {
            Some(name)
        } else {
            None
        }
    })();
    parsed.unwrap_or_else(|| ObjectName::from(vec![Ident::new(value.to_string())]))
}

fn convert_copy_location_to_idents(object_name: &ObjectName) -> Vec<Ident> {
    let location = object_name.to_string();
    if location.contains("gcs://") || location.contains("s3://") || location.contains("azure://") {
        vec![Ident::new(location.replace(['\"', '\''], ""))]
    } else {
        convert_to_idents(object_name)
    }
}

fn convert_to_idents(object_name: &ObjectName) -> Vec<Ident> {
    object_name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => ident.clone(),
            ObjectNamePart::Function(f) => f.name.clone(),
        })
        .collect()
}

pub fn extract_up_to_two_ident_values(
    object_name: &ObjectName,
) -> (Option<String>, Option<String>) {
    let idents = convert_to_idents(object_name);
    let mut idents_iter = idents.iter();

    let first_value = idents_iter.next().map(|ident| ident.value.clone());
    let second_value = idents_iter.next().map(|ident| ident.value.clone());

    (first_value, second_value)
}

#[test]
fn test_extract_up_to_two_ident_values_one_ident() {
    let object_name = ObjectName::from(vec![Ident::new("first")]);
    assert_eq!(
        extract_up_to_two_ident_values(&object_name),
        (Some("first".to_string()), None)
    );
}

#[test]
fn test_extract_up_to_two_ident_values_one_ident_with_quotes() {
    let object_name = ObjectName::from(vec![Ident::with_quote('`', "first")]);
    assert_eq!(
        extract_up_to_two_ident_values(&object_name),
        (Some("first".to_string()), None)
    );
}

#[test]
fn test_extract_up_to_two_ident_values_two_idents() {
    let object_name = ObjectName::from(vec![Ident::new("first"), Ident::with_quote('`', "second")]);
    assert_eq!(
        extract_up_to_two_ident_values(&object_name),
        (Some("first".to_string()), Some("second".to_string()))
    );
}

#[test]
fn test_extract_up_to_two_ident_values_three_idents() {
    let object_name = ObjectName::from(vec![
        Ident::new("first"),
        Ident::with_quote('`', "second"),
        Ident::new("third"),
    ]);
    assert_eq!(
        extract_up_to_two_ident_values(&object_name),
        (Some("first".to_string()), Some("second".to_string()))
    );
}

#[test]
fn test_extract_up_to_two_ident_values_empty_idents() {
    let object_name = ObjectName(vec![]);
    assert_eq!(extract_up_to_two_ident_values(&object_name), (None, None));
}
