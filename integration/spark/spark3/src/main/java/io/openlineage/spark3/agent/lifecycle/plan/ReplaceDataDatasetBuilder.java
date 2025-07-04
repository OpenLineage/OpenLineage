/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * {@link LogicalPlan} visitor that matches an {@link ReplaceData} commands and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class ReplaceDataDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<ReplaceData> {

  private final DatasetFactory<OpenLineage.OutputDataset> factory;

  public ReplaceDataDatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.OutputDataset> factory) {
    super(context, false);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof ReplaceData;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, ReplaceData x) {
    // Needs to cast to logical plan despite IntelliJ claiming otherwise.
    LogicalPlan logicalPlan = (LogicalPlan) ((ReplaceData) x).table();

    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            logicalPlan,
            ScalaConversionUtils.toScalaFn(
                (lp) -> Collections.<OpenLineage.OutputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }

  @Override
  public Optional<String> jobNameSuffix(ReplaceData plan) {
    if (plan.table() instanceof DataSourceV2Relation) {
      return new DataSourceV2RelationOutputDatasetBuilder(context, factory)
          .jobNameSuffix((DataSourceV2Relation) (plan.table()));
    } else {
      return Optional.ofNullable(plan.table()).map(t -> t.name());
    }
  }
}
