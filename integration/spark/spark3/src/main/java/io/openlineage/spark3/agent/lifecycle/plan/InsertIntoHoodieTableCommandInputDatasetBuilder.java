/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Extracts the input datasets of Hudi's {@code InsertIntoHoodieTableCommand} (Hudi SQL {@code
 * INSERT INTO/OVERWRITE}). The source query is not a child of the command, so it is traversed
 * explicitly with the other input builders.
 */
public class InsertIntoHoodieTableCommandInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public InsertIntoHoodieTableCommandInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return HudiUtils.isInsertIntoHoodieTableCommand(x);
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    PartialFunction<LogicalPlan, Collection<InputDataset>> delegateFn =
        delegate(
            context.getInputDatasetQueryPlanVisitors(), context.getInputDatasetBuilders(), event);
    return HudiUtils.insertQuery(x)
        .map(query -> ScalaConversionUtils.fromSeq(query.collect(delegateFn)))
        .orElse(Collections.emptyList())
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
