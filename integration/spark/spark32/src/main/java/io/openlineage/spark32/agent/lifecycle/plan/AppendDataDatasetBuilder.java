/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

import java.util.Collections;
import java.util.List;

/**
 * {@link LogicalPlan} visitor that matches an {@link AppendData} commands and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class AppendDataDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<AppendData> {

  private final DatasetFactory<OpenLineage.OutputDataset> factory;

  public AppendDataDatasetBuilder(
      OpenLineageContext context, DatasetFactory<OpenLineage.OutputDataset> factory) {
    super(context, false);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof AppendData;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, AppendData x) {
    // Needs to cast to logical plan despite IntelliJ claiming otherwise.
    LogicalPlan logicalPlan = (LogicalPlan) ((AppendData) x).table();

    if (logicalPlan instanceof DataSourceV2Relation) {
      return new DataSourceV2RelationOutputDatasetBuilder(context, factory)
          .apply(event, (DataSourceV2Relation) logicalPlan);
    } else {
      return Collections.emptyList();
    }
  }
}
