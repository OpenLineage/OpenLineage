/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.spark.agent.util.DatasetFacetsUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;

/**
 * Extracts the output dataset of an {@link InsertIntoDataSourceCommand} by delegating its target
 * relation to the registered output dataset builders, and marks it as overwritten when the command
 * overwrites the relation.
 */
public class InsertIntoDataSourceDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<InsertIntoDataSourceCommand> {

  public InsertIntoDataSourceDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof InsertIntoDataSourceCommand;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, InsertIntoDataSourceCommand command) {
    return delegate(command.logicalRelation(), event).stream()
        .map(dataset -> command.overwrite() ? withOverwriteLifecycle(dataset) : dataset)
        .collect(Collectors.toList());
  }

  private OpenLineage.OutputDataset withOverwriteLifecycle(OpenLineage.OutputDataset dataset) {
    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.DatasetFacets facets =
        DatasetFacetsUtils.copyToBuilder(context, dataset.getFacets())
            .lifecycleStateChange(
                openLineage.newLifecycleStateChangeDatasetFacet(
                    LifecycleStateChange.OVERWRITE, null))
            .build();
    return openLineage.newOutputDataset(
        dataset.getNamespace(), dataset.getName(), facets, dataset.getOutputFacets());
  }
}
