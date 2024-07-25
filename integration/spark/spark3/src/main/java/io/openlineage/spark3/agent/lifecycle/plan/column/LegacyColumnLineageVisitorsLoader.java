/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.lifecycle.plan.column.CustomColumnLineageVisitor;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Utility class responsible for loading deprecated `CustomColumnLineageVisitor` implementations and
 * converting them to the new `ColumnLevelLineageVisitor` interface.
 */
public class LegacyColumnLineageVisitorsLoader {

  /**
   * Loads the deprecated `CustomColumnLineageVisitor` implementations using the ServiceLoader and
   * converts them to `ColumnLevelLineageVisitor` instances.
   *
   * @return a list of `ColumnLevelLineageVisitor` instances converted from the deprecated visitors.
   */
  static List<ColumnLevelLineageVisitor> getVisitors() {
    ServiceLoader<CustomColumnLineageVisitor> loader =
        ServiceLoader.load(CustomColumnLineageVisitor.class);

    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                loader.iterator(), Spliterator.IMMUTABLE & Spliterator.DISTINCT),
            false)
        .map(LegacyColumnLineageVisitorsLoader::fromLegacyInterface)
        .collect(Collectors.toList());
  }

  /** Converts a deprecated `CustomColumnLineageVisitor` to a `ColumnLevelLineageVisitor`. */
  private static ColumnLevelLineageVisitor fromLegacyInterface(
      CustomColumnLineageVisitor customVisitor) {
    return new ColumnLevelLineageVisitor() {
      @Override
      public void collectInputs(ColumnLevelLineageContext context, LogicalPlan node) {
        customVisitor.collectInputs(node, context.getBuilder());
      }

      @Override
      public void collectOutputs(ColumnLevelLineageContext context, LogicalPlan node) {
        customVisitor.collectOutputs(node, context.getBuilder());
      }

      @Override
      public void collectExpressionDependencies(
          ColumnLevelLineageContext context, LogicalPlan node) {
        customVisitor.collectExpressionDependencies(node, context.getBuilder());
      }
    };
  }
}
