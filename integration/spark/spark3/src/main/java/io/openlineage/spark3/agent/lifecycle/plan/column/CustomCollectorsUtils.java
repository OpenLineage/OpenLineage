/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import java.util.List;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class CustomCollectorsUtils {

  private static Stream<CustomColumnLineageVisitor> loadCustomCollectors() {
    ServiceLoader<CustomColumnLineageVisitor> loader =
        ServiceLoader.load(CustomColumnLineageVisitor.class);

    List<CustomColumnLineageVisitor> collect =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    loader.iterator(), Spliterator.IMMUTABLE & Spliterator.DISTINCT),
                false)
            .collect(Collectors.toList());

    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(
            loader.iterator(), Spliterator.IMMUTABLE & Spliterator.DISTINCT),
        false);
  }

  static void collectInputs(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCustomCollectors()
        .forEach(collector -> collector.collectInputs(plan, builder));
  }

  static void collectOutputs(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCustomCollectors()
        .forEach(collector -> collector.collectOutputs(plan, builder));
  }

  static void collectExpressionDependencies(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    CustomCollectorsUtils.loadCustomCollectors()
        .forEach(collector -> collector.collectExpressionDependencies(plan, builder));
  }
}
