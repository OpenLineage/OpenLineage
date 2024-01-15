/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

public class SnowflakeEventHandlerFactory implements OpenLineageEventHandlerFactory {

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    // The right function will be determined at runtime by using type checking based on the correct
    // Spark LogicalPlan
    return Collections.singleton((PartialFunction) new QueryPlanDatasetBuilder(context));
  }
}
