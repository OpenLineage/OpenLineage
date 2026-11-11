/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTableWithFilters;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Provides the job name suffix for {@link DeleteFromTableWithFilters}, which the optimizer produces
 * in place of a row-level DELETE plan when the table can delete the matching data using metadata
 * only (for example, when the condition covers whole partitions).
 *
 * <p>The output dataset is not extracted here: it is already extracted from the analyzed plan,
 * which still contains the row-level DELETE plan, and extracting it twice would duplicate it.
 */
public class DeleteFromTableWithFiltersDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<DeleteFromTableWithFilters> {

  public DeleteFromTableWithFiltersDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof DeleteFromTableWithFilters
        && ((DeleteFromTableWithFilters) x).table() instanceof DataSourceV2Relation;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, DeleteFromTableWithFilters x) {
    return Collections.emptyList();
  }

  @Override
  public Optional<String> jobNameSuffix(DeleteFromTableWithFilters x) {
    return Optional.of(((DataSourceV2Relation) x.table()).name());
  }
}
