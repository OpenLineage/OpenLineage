/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;

@Slf4j
public class JdbcColumnLineageVisitor implements ColumnLevelLineageVisitor {

  protected OpenLineageContext context;

  public JdbcColumnLineageVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public void collectInputs(ColumnLevelLineageContext context, LogicalPlan node) {
    if (!isJDBCNode(node)) {
      return;
    }
    JdbcColumnLineageVisitorDelegate delegate =
        new JdbcColumnLineageVisitorDelegate(
            context,
            (JDBCRelation) ((LogicalRelation) node).relation(),
            ScalaConversionUtils.fromSeq(node.output()));

    if (delegate.isDefinedAt()) {
      delegate.collectInputs();
    }
  }

  @Override
  public void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan node) {
    if (!isJDBCNode(node)) {
      return;
    }

    JdbcColumnLineageVisitorDelegate delegate =
        new JdbcColumnLineageVisitorDelegate(
            context,
            (JDBCRelation) ((LogicalRelation) node).relation(),
            ScalaConversionUtils.fromSeq(node.output()));

    if (delegate.isDefinedAt()) {
      delegate.collectExpressionDependencies();
    }
  }

  @Override
  public void collectOutputs(ColumnLevelLineageContext context, LogicalPlan node) {}

  private boolean isJDBCNode(LogicalPlan node) {
    return node instanceof LogicalRelation
        && ((LogicalRelation) node).relation() instanceof JDBCRelation;
  }
}
