/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;

@Slf4j
public class InMemoryRelationInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<InMemoryRelation> {

  public InMemoryRelationInputDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return true;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(InMemoryRelation x) {
    return Collections.emptyList();
  }

  @Override
  public List<OpenLineage.InputDataset> apply(
      SparkListenerEvent event, InMemoryRelation inMemoryRelation) {
    return PlanUtils3.getLogicalPlanOf(context, inMemoryRelation)
        .map(
            plan ->
                ScalaConversionUtils.fromSeq(
                        plan.collect(
                            delegate(
                                context.getInputDatasetQueryPlanVisitors(),
                                context.getInputDatasetBuilders(),
                                event)))
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()))
        .orElse(Collections.<OpenLineage.InputDataset>emptyList());
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof InMemoryRelation;
  }
}
