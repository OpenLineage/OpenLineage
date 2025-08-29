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
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.AggregateNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.CreateTableAsSelectNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.DataSourceV2RelationNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.FilterNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.GenerateNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.IcebergMergeIntoNodesVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.JoinNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.NodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.ProjectNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.SortNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.UnionNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node.WindowNodeVisitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class VisitorFactory {
  List<NodeVisitor> nodeVisitors() {
    return Collections.unmodifiableList(
        Arrays.asList(
            new ProjectNodeVisitor(),
            new GenerateNodeVisitor(),
            new CreateTableAsSelectNodeVisitor(),
            new AggregateNodeVisitor(),
            new JoinNodeVisitor(),
            new FilterNodeVisitor(),
            new SortNodeVisitor(),
            new WindowNodeVisitor(),
            new DataSourceV2RelationNodeVisitor(),
            new UnionNodeVisitor(),
            new IcebergMergeIntoNodesVisitor()));
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
