// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::context::{Context, ContextFrame};
use crate::dialect::allows_implicit_recursive_cte;
use crate::lineage::*;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AccessExpr, AlterTableOperation, CopyIntoSnowflakeKind, CreateTableLikeKind, Expr, FromTable,
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments, Ident, Join,
    JoinConstraint, JoinOperator, MergeAction, MergeInsertKind, MergeUpdateKind,
    MultiTableInsertIntoClause, ObjectName, ObjectNamePart, Query, RenameTableNameKind, Select,
    SelectItem, SetExpr, Statement, Subscript, Table, TableAlias, TableFactor, TableFunctionArgs,
    TableObject, TableWithJoins, UpdateTableFromKind, Use, Value, ValueWithSpan, WindowSpec,
    WindowType, With,
};
use sqlparser::dialect::{
    DatabricksDialect, Dialect, MsSqlDialect, MySqlDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

pub trait Visit {
    fn visit(&self, context: &mut Context) -> Result<()>;
}

impl Visit for With {
    fn visit(&self, context: &mut Context) -> Result<()> {
        let recursive =
            self.recursive || allows_implicit_recursive_cte(context.dialect().as_base());
        for cte in &self.cte_tables {
            if recursive {
                context.declare_recursive_cte(cte.alias.name.clone());
            }
            context.add_table_alias(
                DbTableMeta::new_default_dialect("".to_string()),
                cte.alias.name.clone(),
            );
            context.push_frame();
            cte.query.visit(context)?;
            if let Some(frame) = context.pop_frame() {
                let table = context.table_meta(vec![cte.alias.name.clone()]);
                context.collect_with_table(frame, table);
            }
            let is_dml = matches!(
                *cte.query.body,
                SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_)
            );
            context.adjust_cte_dependencies(cte.alias.name.clone(), recursive, is_dml);
        }
        Ok(())
    }
}

impl Visit for TableFactor {
    fn visit(&self, context: &mut Context) -> Result<()> {
        match self {
            TableFactor::Table { alias, .. } => {
                // Check if table name is provided using IDENTIFIER function,
                // use it if found and valid, otherwise keep the current name
                let Some(effective_idents) = get_table_name_from_table_factor(self, context) else {
                    return Ok(());
                };

                // A reference to a CTE in scope is replaced by the CTE's name
                // as defined, which may differ in case (`orders` for CTE
                // `Orders` in Snowflake).
                let cte = context.cte_named_by(&effective_idents);
                let idents = match &cte {
                    Some(cte) => vec![cte.clone()],
                    None => effective_idents.clone(),
                };

                let table = context.table_meta(idents);
                if let Some(alias) = alias {
                    context.add_table_alias(table.clone(), alias.name.clone());
                }

                if context.is_main() {
                    match cte {
                        Some(cte) => context.mark_table_as_used(&cte),
                        None => context.add_relation_input(table),
                    }
                } else {
                    // A dependency keeps the name as written: `adjust_cte_dependencies`
                    // matches it to a CTE by folded key, and the CTE's spelling would
                    // make `ORDERS` and `public.orders` one table under a default
                    // schema.
                    context.add_table_dependency(effective_idents);
                }

                Ok(())
            }
            TableFactor::Pivot { table, alias, .. } | TableFactor::Unpivot { table, alias, .. } => {
                table.visit(context)?;
                if let (Some(alias), Some(ident)) =
                    (alias, get_table_name_from_table_factor(table, context))
                {
                    let resolved = context.resolve_cte_reference(ident);
                    let meta = context.table_meta(resolved);
                    context.add_table_alias(meta, alias.name.clone());
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
                let frame = context.pop_frame().unwrap();

                if let Some(alias) = alias {
                    let table = context.table_meta(vec![alias.name.clone()]);
                    context.collect_with_table(frame, table);
                } else {
                    context.collect(frame);
                }

                Ok(())
            }
            TableFactor::TableFunction { .. } | TableFactor::Function { .. } => {
                // https://docs.snowflake.com/en/sql-reference/functions-table
                // https://github.com/apache/datafusion-sqlparser-rs/pull/1026/files#r1373705587
                // Skip table functions as we don't support extracting lineage from functions
                Ok(())
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => table_with_joins.visit(context),
            TableFactor::UNNEST { array_exprs, .. } => {
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
                // BigQuery's `UNNEST(...)` is a row generator over an array expression, not a
                // real table, so UNNEST itself contributes no input; still visit array_exprs
                // in case they contain subqueries referencing real tables.
                for array_expr in array_exprs {
                    array_expr.visit(context)?;
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
                if let Some(column_ctx) = context.column_context() {
                    let descendant = column_ctx.name.clone();
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
                if let Some(column_ctx) = context.column_context() {
                    let descendant = column_ctx.name.clone();
                    let (ancestor_ident, qualifier) = ids.split_last().unwrap();
                    let table = context.resolve_column_qualifier(qualifier.to_vec());
                    context.add_column_ancestors(
                        ColumnMeta::new(descendant, None),
                        vec![ColumnMeta::new(ancestor_ident.value.clone(), Some(table))],
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
                subquery.visit(context)?;
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
            FunctionArguments::Subquery(query) => query.visit(context)?,
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

impl Visit for TableWithJoins {
    fn visit(&self, context: &mut Context) -> Result<()> {
        self.relation.visit(context)?;
        for join in &self.joins {
            join.visit(context)?;
        }
        Ok(())
    }
}

fn table_with_joins_relations(table: &TableWithJoins) -> impl Iterator<Item = &TableFactor> {
    std::iter::once(&table.relation).chain(table.joins.iter().map(|join| &join.relation))
}

impl Visit for Select {
    fn visit(&self, context: &mut Context) -> Result<()> {
        // If we're selecting from a single table, that table becomes the default
        if let [t] = self.from.as_slice() {
            if let Some(ident) = get_table_name_from_table_factor(&t.relation, context) {
                let resolved = context.resolve_cte_reference(ident);
                let table = context.table_meta(resolved);
                context.set_table_context(Some(table));
            }
        }

        // The names this SELECT's FROM clause introduces, in scope until the
        // end of the SELECT, even when visiting it fails. A nested query
        // opens its own scope, so it never binds to these.
        let mut names = Vec::new();
        for relation in self.from.iter().flat_map(table_with_joins_relations) {
            collect_correlation_names(relation, context, &mut names);
        }
        let tables_frame = context.with_correlation_names(names, |context| {
            let tables_frame = visit_from(&self.from, context)?;
            visit_select_clauses(self, context)?;
            Ok(tables_frame)
        })?;

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

/// Visits the FROM items of a SELECT and returns the frame holding their
/// tables.
fn visit_from(from: &[TableWithJoins], context: &mut Context) -> Result<ContextFrame> {
    fn visit_item(item: &impl Visit, context: &mut Context) -> Result<()> {
        context.push_frame();
        item.visit(context)?;
        let frame = context.pop_frame().unwrap();
        context.collect_aliases(&frame);
        context.collect(frame);
        Ok(())
    }

    context.push_frame();
    for table in from {
        visit_item(&table.relation, context)?;
        for join in &table.joins {
            visit_item(join, context)?;
        }
    }

    let tables_frame = context.pop_frame().unwrap();
    context.collect_aliases(&tables_frame);
    Ok(tables_frame)
}

/// Appends the names a FROM item introduces for column qualifiers, each with
/// the relation it stands for. An alias stands for itself and hides whatever
/// it renames; an unaliased table stands for its full name (CTE references
/// resolved) under its last name part; an unaliased parenthesized join
/// introduces the names of its members, and an unaliased PIVOT or UNPIVOT
/// those of the table it wraps.
fn collect_correlation_names(
    relation: &TableFactor,
    context: &Context,
    out: &mut Vec<(Ident, Vec<Ident>)>,
) {
    if let Some(alias) = table_factor_alias(relation) {
        out.push((alias.name.clone(), vec![alias.name.clone()]));
        return;
    }
    match relation {
        TableFactor::Table { .. } => {
            if let Some(ident) = get_table_name_from_table_factor(relation, context) {
                let resolved = context.resolve_cte_reference(ident);
                if let Some(last) = resolved.last().cloned() {
                    out.push((last, resolved));
                }
            }
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            for member in table_with_joins_relations(table_with_joins) {
                collect_correlation_names(member, context, out);
            }
        }
        TableFactor::Pivot { table, .. } | TableFactor::Unpivot { table, .. } => {
            collect_correlation_names(table, context, out);
        }
        _ => {}
    }
}

fn table_factor_alias(relation: &TableFactor) -> Option<&TableAlias> {
    match relation {
        TableFactor::Table { alias, .. }
        | TableFactor::Derived { alias, .. }
        | TableFactor::TableFunction { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::UNNEST { alias, .. }
        | TableFactor::JsonTable { alias, .. }
        | TableFactor::OpenJsonTable { alias, .. }
        | TableFactor::NestedJoin { alias, .. }
        | TableFactor::Pivot { alias, .. }
        | TableFactor::Unpivot { alias, .. }
        | TableFactor::MatchRecognize { alias, .. }
        | TableFactor::XmlTable { alias, .. } => alias.as_ref(),
        _ => None,
    }
}

fn visit_select_clauses(select: &Select, context: &mut Context) -> Result<()> {
    for projection in &select.projection {
        match projection {
            SelectItem::UnnamedExpr(expr) => {
                match expr {
                    Expr::Identifier(id) => {
                        context.set_column_context(Some(ColumnMeta::new(id.value.clone(), None)))
                    }
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

    for clause in [&select.selection, &select.having, &select.qualify]
        .into_iter()
        .flatten()
    {
        visit_isolated(context, |ctx| clause.visit(ctx))?;
    }

    Ok(())
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
            SetExpr::Query(q) => q.visit(context),
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

        let Some(with) = &self.with else {
            // Isolate the query body in its own frame so its FROM aliases do
            // not leak into the enclosing statement (e.g. CTAS/CREATE VIEW
            // targets or INSERT ALL WHEN predicates) or sibling set-op arms.
            context.push_frame();
            let result = self.body.visit(context);
            let frame = context.pop_frame().unwrap();
            context.collect(frame);
            return result;
        };

        // Whether the region this query sits in is the main body -
        // the body frame below may only be promoted back to main if
        // it is. A query nested inside a CTE body keeps the enclosing
        // region's flag: its direct table reads are dependencies of
        // that CTE, not inputs of the statement.
        let enclosing_is_main = context.is_main();

        // Keep `with_frame` on the stack while visiting `self.body` so its
        // CTE definitions and aliases are visible inside this query without
        // ever leaking into the enclosing frame.
        context.push_frame();
        context.unset_frame_to_main_body();
        if let Err(err) = with.visit(context) {
            context.pop_frame();
            return Err(err);
        }

        context.push_frame();
        if enclosing_is_main {
            context.set_frame_to_main_body();
        }
        let body_result = self.body.visit(context);

        let mut body_frame = context.pop_frame().unwrap();
        let with_frame = context.pop_frame().unwrap();

        body_result?;

        // Resolve column lineage and CTE inputs/dependencies within this
        // query's scope before merging `body_frame` into the enclosing frame.
        body_frame.coalesce(&with_frame);
        context.resolve_cte_scope(with_frame, &mut body_frame, with.recursive);
        context.collect(body_frame);

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
                    visit_isolated(context, |ctx| when_clause.condition.visit(ctx))?;
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
                        if let Some(idents) =
                            resolve_insert_target_name(name, &insert.columns, context)
                        {
                            context.add_insert_output(idents);
                        }
                    }
                    TableObject::TableFunction(func) => {
                        func.visit(context)?;
                    }
                    TableObject::TableQuery(_) => {}
                }
            }
            Statement::Merge(merge) => {
                if let Some(table_name) = get_table_name_from_table_factor(&merge.table, context) {
                    context.add_output(table_name);
                }
                merge.source.visit(context)?;
                visit_isolated(context, |ctx| merge.on.visit(ctx))?;
                for clause in &merge.clauses {
                    visit_isolated(context, |ctx| {
                        if let Some(predicate) = &clause.predicate {
                            predicate.visit(ctx)?;
                        }
                        match &clause.action {
                            MergeAction::Insert(insert) => {
                                if let MergeInsertKind::Values(values) = &insert.kind {
                                    for row in &values.rows {
                                        for expr in &row.content {
                                            expr.visit(ctx)?;
                                        }
                                    }
                                }
                                if let Some(predicate) = &insert.insert_predicate {
                                    predicate.visit(ctx)?;
                                }
                            }
                            MergeAction::Update(update) => {
                                if let MergeUpdateKind::Set(assignments) = &update.kind {
                                    for assignment in assignments {
                                        assignment.value.visit(ctx)?;
                                    }
                                }
                                if let Some(predicate) = &update.update_predicate {
                                    predicate.visit(ctx)?;
                                }
                                if let Some(predicate) = &update.delete_predicate {
                                    predicate.visit(ctx)?;
                                }
                            }
                            MergeAction::Delete { .. } | MergeAction::DoNothing { .. } => {}
                        }
                        Ok(())
                    })?;
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
                if let Some(url) = &stage_params.url {
                    context.add_non_table_input(vec![Ident::new(url.to_string())], true, true);
                }
                context.add_non_table_output(convert_to_idents(name), false, true);
            }
            Statement::Update(update) => {
                if let Some(name) =
                    get_table_name_from_table_factor(&update.table.relation, context)
                {
                    context.add_output(name);
                }

                for join in &update.table.joins {
                    join.visit(context)?;
                }

                if let Some(
                    UpdateTableFromKind::BeforeSet(tables) | UpdateTableFromKind::AfterSet(tables),
                ) = &update.from
                {
                    for table in tables {
                        table.visit(context)?;
                    }
                }
                for assignment in &update.assignments {
                    assignment.value.visit(context)?;
                }
                if let Some(expr) = &update.selection {
                    visit_isolated(context, |ctx| expr.visit(ctx))?;
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
                    for relation in tables.iter().flat_map(table_with_joins_relations) {
                        if let Some(output) = get_table_name_from_table_factor(relation, context) {
                            context.add_output(output);
                        }
                    }
                } else {
                    let existing_inputs = context.inputs.clone();
                    for table in tables {
                        table.visit(context)?;
                    }
                    for table in &delete.tables {
                        context.move_input_to_output(convert_to_idents(table));
                    }
                    context.inputs.extend(existing_inputs);
                }

                if let Some(using) = &delete.using {
                    for table in using {
                        table.visit(context)?;
                    }
                }

                if let Some(expr) = &delete.selection {
                    visit_isolated(context, |ctx| expr.visit(ctx))?;
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
                    context.set_default_database(first_id);
                    context.set_default_schema(second_id);
                } else if first_id.is_some() {
                    // One id is present, each dialect can work differently
                    match use_enum {
                        Use::Catalog(_) => {
                            context.set_default_database(first_id); // Databricks specific
                        }
                        Use::Schema(_) => {
                            context.set_default_schema(first_id); // Snowflake specific
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

        let frame = context.pop_frame().unwrap();
        if context.is_main() {
            context.collect_inputs(&frame);
        }
        context.collect(frame);

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

/// Visits a predicate or action clause in a temporary frame so subqueries
/// contribute table inputs/dependencies without leaking projected columns or
/// local aliases into the enclosing frame.
fn visit_isolated(context: &mut Context, f: impl FnOnce(&mut Context) -> Result<()>) -> Result<()> {
    context.push_frame();
    let result = f(context);
    let frame = context.pop_frame().unwrap();
    context.collect_dependencies(frame);
    result
}

/// Register one multi-table insert clause target (an INTO clause of a
/// Snowflake/Oracle INSERT ALL or INSERT FIRST) as an output.
fn add_multi_table_target(clause: &MultiTableInsertIntoClause, context: &mut Context) {
    if let Some(idents) = resolve_effective_table_name(&clause.table_name, &None, context) {
        context.add_insert_output(idents);
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
) -> Option<Vec<Ident>> {
    let dialect = context.dialect().as_base();
    let is_mssql_or_databricks = dialect.is::<DatabricksDialect>() || dialect.is::<MsSqlDialect>();
    let is_bare_identifier = is_mssql_or_databricks
        && matches!(
            name.0.as_slice(),
            [ObjectNamePart::Identifier(part)] if part.value.eq_ignore_ascii_case("identifier")
        );
    if is_bare_identifier && !columns.is_empty() {
        // The target carries something in its column list. For an
        // IDENTIFIER(...) target that is the call's argument, spilled
        // here by sqlparser; a single-quoted string is the one
        // argument form that resolves to a table name.
        let pseudo_column = match columns {
            [column] => match column.0.as_slice() {
                [ObjectNamePart::Identifier(part)] if part.quote_style == Some('\'') => {
                    Some(part.value.as_str())
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(value) = pseudo_column {
            let resolved = parse_identifier_string(value, dialect);
            return (!is_empty_idents(&resolved)).then_some(resolved);
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
    resolve_effective_table_name(name, &None, context)
}

/// Whether an insert column is a plain column name, as opposed to an
/// IDENTIFIER(...) argument that sqlparser spilled into the column
/// list: one unquoted, bracket-quoted or backtick-quoted identifier.
fn is_plain_column_name(column: &ObjectName) -> bool {
    matches!(
        column.0.as_slice(),
        [ObjectNamePart::Identifier(part)]
            if matches!(part.quote_style, None | Some('[') | Some('`'))
    )
}

fn get_table_name_from_table_factor(table: &TableFactor, context: &Context) -> Option<Vec<Ident>> {
    if let TableFactor::Table { name, args, .. } = table {
        resolve_effective_table_name(name, args, context)
    } else {
        None
    }
}

fn resolve_effective_table_name(
    name: &ObjectName,
    args: &Option<TableFunctionArgs>,
    context: &Context,
) -> Option<Vec<Ident>> {
    let dialect = context.dialect().as_base();
    let idents = if dialect.is::<DatabricksDialect>() || dialect.is::<MsSqlDialect>() {
        let is_identifier_function = matches!(
            name.0.as_slice(),
            [ObjectNamePart::Identifier(ident)] if ident.value.eq_ignore_ascii_case("identifier")
        );
        if is_identifier_function {
            if let Some(args) = args {
                if args.args.len() != 1 {
                    return None;
                }
                identifier_function_arg(&args.args, dialect)?
            } else {
                // With no arguments this is a plain table named `identifier`.
                convert_to_idents(name)
            }
        } else {
            convert_to_idents(name)
        }
    } else if dialect.is::<SnowflakeDialect>() {
        match name.0.first()? {
            ObjectNamePart::Function(f) if f.name.value.eq_ignore_ascii_case("identifier") => {
                identifier_function_arg(&f.args, dialect)?
            }
            _ => convert_to_idents(name),
        }
    } else {
        convert_to_idents(name)
    };

    // An empty or blank name (e.g. `""`, `IDENTIFIER('')`, or the empty
    // placeholder multi-table inserts leave on `insert.table`) names nothing.
    (!is_empty_idents(&idents)).then_some(idents)
}

fn is_empty_idents(idents: &[Ident]) -> bool {
    idents.is_empty() || matches!(idents, [ident] if ident.value.trim().is_empty())
}

fn identifier_function_arg(args: &[FunctionArg], dialect: &dyn Dialect) -> Option<Vec<Ident>> {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = args.first()? else {
        return None;
    };
    match expr {
        Expr::Identifier(ident) => Some(vec![ident.clone()]),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(ident),
            ..
        }) => Some(parse_identifier_string(ident, dialect)),
        _ => None,
    }
}

/// The string argument of IDENTIFIER(...) is itself an object name:
/// IDENTIFIER('db.schema.t1') refers to table t1 in db.schema, not to a
/// table literally named "db.schema.t1". Parse it with the statement's
/// dialect so quoted components keep their quoting and case. A string that
/// does not parse as one complete object name keeps the historical
/// treatment of the whole string as a single identifier.
fn parse_identifier_string(value: &str, dialect: &dyn Dialect) -> Vec<Ident> {
    let parsed = (|| {
        let mut parser = Parser::new(dialect).try_with_sql(value).ok()?;
        let name = parser.parse_object_name(false).ok()?;
        let fully_consumed = parser.peek_token().token == Token::EOF;
        let all_identifiers = name
            .0
            .iter()
            .all(|part| matches!(part, ObjectNamePart::Identifier(_)));
        if fully_consumed && all_identifiers && !name.0.is_empty() {
            Some(convert_to_idents(&name))
        } else {
            None
        }
    })();
    parsed.unwrap_or_else(|| vec![Ident::new(value.to_string())])
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
