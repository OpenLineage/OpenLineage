/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
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
                false)
            .map(
                customVisitor ->
                    new ColumnLevelLineageVisitor() {
                      @Override
                      public void collectInputs(
                          ColumnLevelLineageContext context, LogicalPlan node) {
                        customVisitor.collectInputs(node, context.getBuilder());
                      }

                      @Override
                      public void collectOutputs(
                          ColumnLevelLineageContext context, LogicalPlan node) {
                        customVisitor.collectOutputs(node, context.getBuilder());
                      }

                      @Override
                      public void collectExpressionDependencies(
                          ColumnLevelLineageContext context, LogicalPlan node) {
                        customVisitor.collectExpressionDependencies(node, context.getBuilder());
                      }
                    }),
        context.getColumnLevelLineageVisitors().stream());
  }

  static void collectInputs(ColumnLevelLineageContext context, LogicalPlan plan) {
    CustomCollectorsUtils.loadCollectors(context.getOlContext())
        .forEach(collector -> collector.collectInputs(context, plan));
  }

  static void collectOutputs(ColumnLevelLineageContext context, LogicalPlan plan) {
    CustomCollectorsUtils.loadCollectors(context.getOlContext())
        .forEach(collector -> collector.collectOutputs(context, plan));
  }

  static void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan plan) {
    CustomCollectorsUtils.loadCollectors(context.getOlContext())
        .forEach(collector -> collector.collectExpressionDependencies(context, plan));
  }
}
