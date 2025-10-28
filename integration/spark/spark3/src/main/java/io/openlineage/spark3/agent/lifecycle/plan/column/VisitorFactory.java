/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.AggregateExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.AliasVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.CaseWhenVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.CoalesceVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.ExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.IfVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.WindowVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.AggregateVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.CreateTableAsSelectVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.DataSourceV2RelationVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.DistinctVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.ExpandVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.FilterVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.GenerateVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.IcebergMergeIntoVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.JoinVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.OperatorVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.ProjectVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.SortVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.UnionVisitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class VisitorFactory {
  List<OperatorVisitor> operatorVisitors() {
    return Collections.unmodifiableList(
        Arrays.asList(
            new ProjectVisitor(),
            new GenerateVisitor(),
            new CreateTableAsSelectVisitor(),
            new DistinctVisitor(),
            new AggregateVisitor(),
            new JoinVisitor(),
            new FilterVisitor(),
            new SortVisitor(),
            new io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.WindowVisitor(),
            new DataSourceV2RelationVisitor(),
            new UnionVisitor(),
            new ExpandVisitor(),
            new IcebergMergeIntoVisitor()));
  }

  List<ExpressionVisitor> expressionVisitors() {
    return Collections.unmodifiableList(
        Arrays.asList(
            new AliasVisitor(),
            new CaseWhenVisitor(),
            new IfVisitor(),
            new CoalesceVisitor(),
            new AggregateExpressionVisitor(),
            new WindowVisitor()));
  }
}
