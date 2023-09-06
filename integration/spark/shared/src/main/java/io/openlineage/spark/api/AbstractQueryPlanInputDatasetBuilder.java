/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

/**
 * {@link AbstractQueryPlanDatasetBuilder} serves as an extension of {@link
 * AbstractQueryPlanDatasetBuilder} which gets applied only for START OpenLineage events. Filtering
 * is done by verifying subclass of {@link SparkListenerEvent}.
 *
 * @param <P>
 */
public abstract class AbstractQueryPlanInputDatasetBuilder<P extends LogicalPlan>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, P, OpenLineage.InputDataset> {

  public AbstractQueryPlanInputDatasetBuilder(
      OpenLineageContext context, boolean searchDependencies) {
    super(context, searchDependencies);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return event instanceof SparkListenerJobStart
        || event instanceof SparkListenerSQLExecutionStart;
  }

  @Override
  public List<InputDataset> apply(P logicalPlan) {
    throw new UnsupportedOperationException("Unimplemented");
  }
}
