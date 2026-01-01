/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.column.QueryRelationColumnLineageCollector;
import io.openlineage.spark3.agent.utils.ExtensionDataSourceV2Utils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/** Extracts expression dependencies from a DataSourceV2Relation operator in {@link LogicalPlan}. */
public class DataSourceV2RelationVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof DataSourceV2Relation
        && ExtensionDataSourceV2Utils.hasQueryExtensionLineage((DataSourceV2Relation) operator);
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    QueryRelationColumnLineageCollector.extractExpressionsFromQuery(builder, operator);
  }
}
