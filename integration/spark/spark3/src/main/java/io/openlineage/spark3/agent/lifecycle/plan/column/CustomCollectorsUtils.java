/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.lifecycle.plan.column.CustomColumnLineageVisitor;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@Slf4j
public class CustomCollectorsUtils {

  private static Stream<ColumnLevelLineageVisitor> loadCollectors(OpenLineageContext context) {
    ServiceLoader<CustomColumnLineageVisitor> loader =
        ServiceLoader.load(CustomColumnLineageVisitor.class);

    return Stream.concat(
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                loader.iterator(), Spliterator.IMMUTABLE & Spliterator.DISTINCT),
            false),
        context.getColumnLevelLineageVisitors().stream());
  }

  static void collectInputs(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCollectors(context)
        .forEach(collector -> collector.collectInputs(plan, builder));
  }

  static void collectOutputs(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCollectors(context)
        .forEach(collector -> collector.collectOutputs(plan, builder));
  }

  static void collectExpressionDependencies(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCollectors(context)
        .forEach(collector -> collector.collectExpressionDependencies(plan, builder));
  }
}
