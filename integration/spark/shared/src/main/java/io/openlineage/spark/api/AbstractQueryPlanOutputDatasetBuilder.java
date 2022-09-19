/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * {@link AbstractQueryPlanDatasetBuilder} serves as an extension of {@link
 * AbstractQueryPlanDatasetBuilder} which gets applied only for COMPLETE OpenLineage events.
 * Filtering is done by verifying subclass of {@link SparkListenerEvent}
 *
 * @param <P>
 */
public abstract class AbstractQueryPlanOutputDatasetBuilder<P extends LogicalPlan>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, P, OpenLineage.OutputDataset> {

  public AbstractQueryPlanOutputDatasetBuilder(
      OpenLineageContext context, boolean searchDependencies) {
    super(context, searchDependencies);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return true;
  }

  protected boolean includeDatasetVersion(SparkListenerEvent event) {
    return event instanceof SparkListenerSQLExecutionEnd;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(P logicalPlan) {
    throw new UnsupportedOperationException("Unimplemented");
  }
}
