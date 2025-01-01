// Copyright 2018-2025 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::context::Context;
use crate::lineage::*;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AlterTableOperation, Expr, FromTable, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, ObjectName, Query, Select, SelectItem, SetExpr, Statement, Table,
    TableFactor, TableFunctionArgs, Use, Value, WindowSpec, WindowType, With,
};
use sqlparser::dialect::{DatabricksDialect, MsSqlDialect, SnowflakeDialect};

pub trait Visit {
    fn visit(&self, context: &mut Context) -> Result<()>;
}

impl Visit for With {
    fn visit(&self, context: &mut Context) -> Result<()> {
        let size = self.cte_tables.len();
        for i in 0..size {
            context.add_table_alias(
                DbTableMeta::new_default_dialect("".to_string()),
                vec![self.cte_tables[i].alias.name.clone()],
            );
            context.push_frame();
            self.cte_tables[i].query.visit(context)?;
            let frame = context.pop_frame();
            if let Some(f) = frame {
                let table = DbTableMeta::new(
                    vec![self.cte_tables[i].alias.name.clone()],
                    context.dialect(),
                    context.default_schema().clone(),
                    context.default_database().clone(),
                );
                context.collect_with_table(f, table);
            }

            if i == 0 {
                context
                    .collect_lower_nested_dependencies(self.cte_tables[i].alias.name.clone().value);
            }

            context.adjust_cte_dependencies(self.cte_tables[i].alias.name.clone().value);
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
                let effective_name =
                    if is_identifier_function_used_to_provide_table_name(name, args, context) {
                        if let Some(table_name) = get_table_name_from_identifier_function(args) {
                            table_name
                        } else {
                            // Exit if table provided using identifier func but couldn't parse it
                            return Ok(());
                        }
                    } else {
                        name.clone()
                    };

                let table = DbTableMeta::new(
                    effective_name.clone().0,
                    context.dialect(),
                    context.default_schema().clone(),
                    context.default_database().clone(),
                );
                if let Some(alias) = alias {
                    context.add_table_alias(table, vec![alias.name.clone()]);
                }

                if !context.is_main() {
                    context.add_table_dependency(effective_name.clone().0);
                }

                if context.is_main() && !context.has_alias(effective_name.to_string()) {
                    context.add_input(effective_name.clone().0);
                }

                if context.is_main() && context.has_alias(effective_name.to_string()) {
                    context.mark_table_as_used(effective_name.clone().0);
                }

                Ok(())
            }
            TableFactor::Pivot { table, alias, .. } => {
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
                    context.add_input(ident);
                }
                Ok(())
            }
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
            } => {
                context.push_frame();
                subquery.visit(context)?;
                let frame = context.pop_frame().unwrap();

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
            | Expr::IsNotUnknown(expr) => {
                expr.visit(context)?;
            }
            Expr::AnyOp {
                left,
                compare_op: _,
                right,
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
                ..
            }
            | Expr::ILike {
                negated: _,
                expr,
                pattern,
                ..
            }
            | Expr::SimilarTo {
                negated: _,
                expr,
                pattern,
                ..
            } => {
                expr.visit(context)?;
                pattern.visit(context)?;
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
            Expr::MapAccess { column, keys } => {
                column.visit(context)?;
                for key in keys {
                    key.key.visit(context)?;
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
            Expr::CompositeAccess { expr, .. } => {
                expr.visit(context)?;
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
            FunctionArguments::Subquery(_) => {}
            FunctionArguments::List(arguments) => {
                for arg in &arguments.args {
                    arg.visit(context)?;
                }
            }
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

impl Visit for Select {
    fn visit(&self, context: &mut Context) -> Result<()> {
        // If we're selecting from a single table, that table becomes the default
        if self.from.len() == 1 {
            let t = self.from.first().unwrap();
            if let TableFactor::Table { name, alias, .. } = &t.relation {
                let table = DbTableMeta::new(
                    name.clone().0,
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
                join.relation.visit(context)?;
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

        if let Some(into) = &self.into {
            context.add_output(into.name.clone().0)
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
            SetExpr::Values(_) => Ok(()),
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
        }
    }
}

impl Visit for Query {
    fn visit(&self, context: &mut Context) -> Result<()> {
        context.push_frame();
        if self.with.is_some() {
            context.unset_frame_to_main_body();
        }

        if let Some(with) = &self.with {
            with.visit(context)?
        }
        let with_frame = context.pop_frame().unwrap();

        context.collect_aliases(&with_frame);

        context.push_frame();
        if self.with.is_some() {
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
                context.add_output(insert.table_name.clone().0);
            }
            Statement::Merge { table, source, .. } => {
                if let Some(table_name) = get_table_name_from_table_factor(table, &*context) {
                    context.add_output(table_name);
                }
                source.visit(context)?;
            }
            Statement::CreateTable(ct) => {
                if let Some(query) = &ct.query {
                    query.visit(context)?;
                }
                if let Some(like_table) = &ct.like {
                    context.add_input(like_table.clone().0);
                }
                if let Some(clone) = &ct.clone {
                    context.add_input(clone.clone().0);
                }

                context.add_output(ct.name.clone().0);
            }
            Statement::CreateView { name, query, .. } => {
                query.visit(context)?;
                context.add_output(name.clone().0);
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
                context.add_non_table_output(name.clone().0, false, true);
            }
            Statement::Update {
                table,
                assignments: _,
                from,
                selection,
                ..
            } => {
                if let Some(name) = get_table_name_from_table_factor(&table.relation, &*context) {
                    context.add_output(name);
                }

                if let Some(src) = from {
                    src.relation.visit(context)?;
                    for join in &src.joins {
                        join.relation.visit(context)?;
                    }
                }
                if let Some(expr) = selection {
                    expr.visit(context)?;
                }
            }
            Statement::AlterTable {
                name,
                if_exists: _,
                only: _,
                operations,
                location: _,
                on_cluster: _,
            } => {
                for operation in operations {
                    match operation {
                        AlterTableOperation::SwapWith { table_name } => {
                            // both table names are inputs and outputs of the swap operation
                            context.add_input(table_name.clone().0);
                            context.add_input(name.clone().0);

                            context.add_output(table_name.clone().0);
                            context.add_output(name.clone().0);
                        }
                        AlterTableOperation::RenameTable { table_name } => {
                            context.add_input(name.clone().0);
                            context.add_output(table_name.clone().0);
                        }
                        _ => context.add_output(name.clone().0),
                    }
                }
            }
            Statement::Delete(delete) => {
                match &delete.from {
                    FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                        for table in tables {
                            if let Some(output) =
                                get_table_name_from_table_factor(&table.relation, &*context)
                            {
                                context.add_output(output);
                            }
                            for join in &table.joins {
                                if let Some(join_output) =
                                    get_table_name_from_table_factor(&join.relation, &*context)
                                {
                                    context.add_output(join_output);
                                }
                            }
                        }
                    }
                }

                if let Some(using) = &delete.using {
                    for table in using {
                        table.relation.visit(context)?;
                        for join in &table.joins {
                            join.relation.visit(context)?;
                        }
                    }
                }

                if let Some(expr) = &delete.selection {
                    expr.visit(context)?;
                }
            }
            Statement::Truncate { table_name, .. } => context.add_output(table_name.clone().0),
            Statement::Drop { names, .. } => {
                for name in names {
                    context.add_output(name.clone().0)
                }
            }
            Statement::CopyIntoSnowflake {
                into, from_stage, ..
            } => {
                context.add_output(into.clone().0);
                if from_stage.to_string().contains("gcs://")
                    || from_stage.to_string().contains("s3://")
                    || from_stage.to_string().contains("azure://")
                {
                    context.add_non_table_input(
                        vec![Ident::new(from_stage.to_string().replace(['\"', '\''], ""))], // just unquoted location URL with,
                        true,
                        true,
                    );
                } else {
                    // Stage
                    context.add_non_table_input(from_stage.clone().0, true, true);
                };
            }
            Statement::Use(use_enum) => {
                // We expect either one id (USE [...] foo;) or two ids (USE [...] foo.bar;)
                let (first_id, second_id) = match use_enum {
                    Use::Catalog(object_name)
                    | Use::Schema(object_name)
                    | Use::Database(object_name)
                    | Use::Object(object_name) => extract_up_to_two_ident_values(object_name),
                    _ => return Ok(()),
                };

                if first_id.is_none() && second_id.is_none() {
                    return Ok(()); // Should never happen
                }

                // Two ids are present, set db and schema
                if first_id.is_some() && second_id.is_some() {
                    context.set_default_database(first_id.clone());
                    context.set_default_schema(second_id.clone());
                    return Ok(());
                }

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
            _ => {}
        }

        let frame = context.pop_frame().unwrap();
        let cte_deps = frame.cte_dependencies.clone();
        let deps = frame.dependencies.clone();
        context.collect_inputs(cte_deps, deps);
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

fn get_table_name_from_table_factor(table: &TableFactor, context: &Context) -> Option<Vec<Ident>> {
    if let TableFactor::Table { name, args, .. } = table {
        if is_identifier_function_used_to_provide_table_name(name, args, context) {
            get_table_name_from_identifier_function(args).map(|table_name| table_name.clone().0)
        } else {
            Some(name.clone().0)
        }
    } else {
        None
    }
}

fn is_identifier_function_used_to_provide_table_name(
    name: &ObjectName,
    args: &Option<TableFunctionArgs>,
    context: &Context,
) -> bool {
    (context.dialect().as_base().is::<DatabricksDialect>()
        || context.dialect().as_base().is::<SnowflakeDialect>()
        || context.dialect().as_base().is::<MsSqlDialect>())
        && name.0.first().unwrap().value.to_lowercase() == "identifier"
        && args.is_some()
}

fn get_table_name_from_identifier_function(args: &Option<TableFunctionArgs>) -> Option<ObjectName> {
    let args = args.as_ref()?; // Return None if there are no args
    if args.args.len() != 1 {
        return None; // Return None if the length is not 1, we do not support it yet
    }

    match &args.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
            Expr::Identifier(ident) => Some(ObjectName(vec![ident.clone()])),
            Expr::Value(Value::SingleQuotedString(ident)) => {
                Some(ObjectName(vec![Ident::new(ident.to_string())]))
            }
            _ => None,
        },
        _ => None,
    }
}

pub fn extract_up_to_two_ident_values(
    object_name: &ObjectName,
) -> (Option<String>, Option<String>) {
    let mut idents_iter = object_name.0.iter();

    let first_value = idents_iter.next().map(|ident| ident.value.clone());
    let second_value = idents_iter.next().map(|ident| ident.value.clone());

    (first_value, second_value)
}

#[test]
fn test_extract_up_to_two_ident_values_one_ident() {
    let object_name = ObjectName(vec![Ident::new("first")]);
    assert_eq!(
        extract_up_to_two_ident_values(&object_name),
        (Some("first".to_string()), None)
    );
}

#[test]
fn test_extract_up_to_two_ident_values_one_ident_with_quotes() {
    let object_name = ObjectName(vec![Ident::with_quote('`', "first")]);
    assert_eq!(
        extract_up_to_two_ident_values(&object_name),
        (Some("first".to_string()), None)
    );
}

#[test]
fn test_extract_up_to_two_ident_values_two_idents() {
    let object_name = ObjectName(vec![Ident::new("first"), Ident::with_quote('`', "second")]);
    assert_eq!(
        extract_up_to_two_ident_values(&object_name),
        (Some("first".to_string()), Some("second".to_string()))
    );
}

#[test]
fn test_extract_up_to_two_ident_values_three_idents() {
    let object_name = ObjectName(vec![
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
