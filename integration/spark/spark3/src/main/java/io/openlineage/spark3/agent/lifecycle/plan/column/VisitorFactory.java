/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.AggregateNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.CreateTableAsSelectNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.DataSourceV2RelationNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.ExpressionVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.FilterNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.GenerateNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.IcebergMergeIntoNodesVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.JoinNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.NodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.ProjectNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.SortNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.UnionNodeVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.WindowNodeVisitor;
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
    return Collections.unmodifiableList(Arrays.asList());
  }
}
