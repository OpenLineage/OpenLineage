/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan.column;

import static io.openlineage.spark.agent.vendor.snowflake.Constants.SNOWFLAKE_CLASS_NAME;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

@Slf4j
public class SnowflakeColumnLineageVisitor implements ColumnLevelLineageVisitor {
  protected OpenLineageContext context;

  public SnowflakeColumnLineageVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public void collectInputs(ColumnLevelLineageContext context, LogicalPlan node) {
    if (isNotSnowflakeNode(node)) {
      return;
    }
    SnowflakeColumnLineageVisitorDelegate delegate =
        new SnowflakeColumnLineageVisitorDelegate(
            context,
            (SnowflakeRelation) ((LogicalRelation) node).relation(),
            ScalaConversionUtils.fromSeq(node.output()));

    if (delegate.isDefinedAt()) {
      delegate.collectInputs();
    }
  }

  @Override
  public void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan node) {
    if (isNotSnowflakeNode(node)) {
      return;
    }
    SnowflakeColumnLineageVisitorDelegate delegate =
        new SnowflakeColumnLineageVisitorDelegate(
            context,
            (SnowflakeRelation) ((LogicalRelation) node).relation(),
            ScalaConversionUtils.fromSeq(node.output()));
    if (delegate.isDefinedAt()) {
      delegate.collectExpressionDependencies();
    }
  }

  @Override
  public void collectOutputs(ColumnLevelLineageContext context, LogicalPlan node) {}

  private boolean isNotSnowflakeNode(LogicalPlan node) {
    if (!(node instanceof LogicalRelation)) {
      return true;
    }

    Object relation = ((LogicalRelation) node).relation();
    log.debug("Checking if relation is Snowflake: {}", relation.getClass().getName());
    try {
      Class<?> c = Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_CLASS_NAME);
      boolean isSnowflake = c.isAssignableFrom(relation.getClass());
      log.debug(
          "Is Snowflake relation: {} (expected: {}, actual: {})",
          isSnowflake,
          SNOWFLAKE_CLASS_NAME,
          relation.getClass().getName());
      return !isSnowflake;
    } catch (Exception e) {
      log.debug("Not a Snowflake relation");
      // swallow - not a snowflake class
    }
    return true;
  }
}
