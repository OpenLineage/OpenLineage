/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.column.QueryRelationColumnLineageCollector;
import io.openlineage.spark3.agent.utils.ExtensionDataSourceV2Utils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/** Extracts expression dependencies from a DataSourceV2Relation node in {@link LogicalPlan}. */
public class DataSourceV2RelationNodeVisitor implements ExpressionDependencyVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof DataSourceV2Relation
        && ExtensionDataSourceV2Utils.hasQueryExtensionLineage((DataSourceV2Relation) plan);
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    QueryRelationColumnLineageCollector.extractExpressionsFromQuery(builder, plan);
  }
}
