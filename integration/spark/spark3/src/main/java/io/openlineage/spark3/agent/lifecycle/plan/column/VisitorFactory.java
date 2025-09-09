/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.AggregateExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.AliasExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.CaseWhenExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.CoalesceExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.ExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.IfExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.WindowExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.AggregateOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.CreateTableAsSelectOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.DataSourceV2RelationOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.DistinctOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.FilterOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.GenerateOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.IcebergMergeIntoOperatorsVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.JoinOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.OperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.ProjectOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.SortOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.UnionOperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.WindowOperatorVisitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class VisitorFactory {
  List<OperatorVisitor> operatorVisitors() {
    return Collections.unmodifiableList(
        Arrays.asList(
            new ProjectOperatorVisitor(),
            new GenerateOperatorVisitor(),
            new CreateTableAsSelectOperatorVisitor(),
            new DistinctOperatorVisitor(),
            new AggregateOperatorVisitor(),
            new JoinOperatorVisitor(),
            new FilterOperatorVisitor(),
            new SortOperatorVisitor(),
            new WindowOperatorVisitor(),
            new DataSourceV2RelationOperatorVisitor(),
            new UnionOperatorVisitor(),
            new IcebergMergeIntoOperatorsVisitor()));
  }

  List<ExpressionVisitor> expressionVisitors() {
    return Collections.unmodifiableList(
        Arrays.asList(
            new AliasExpressionVisitor(),
            new CaseWhenExpressionVisitor(),
            new IfExpressionVisitor(),
            new CoalesceExpressionVisitor(),
            new AggregateExpressionVisitor(),
            new WindowExpressionVisitor()));
  }
}
