/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.SnowflakeRelationDatasetBuilder;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan.column.SnowflakeColumnLineageVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

public class SnowflakeEventHandlerFactory implements OpenLineageEventHandlerFactory {

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>>
      createInputDatasetBuilder(OpenLineageContext context) {
    return Collections.singleton(
        (PartialFunction)
            new SnowflakeRelationDatasetBuilder<>(context, DatasetFactory.input(context), true));
  }

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    // The right function will be determined at runtime by using type checking based on the correct
    // Spark LogicalPlan
    return Arrays.asList(
        (PartialFunction) new SnowflakeSaveIntoDataSourceCommandDatasetBuilder(context),
        (PartialFunction)
            new SnowflakeRelationDatasetBuilder<>(context, DatasetFactory.output(context), false));
  }

  @Override
  public Collection<ColumnLevelLineageVisitor> createColumnLevelLineageVisitors(
      OpenLineageContext context) {
    return Collections.singleton(new SnowflakeColumnLineageVisitor(context));
  }
}
