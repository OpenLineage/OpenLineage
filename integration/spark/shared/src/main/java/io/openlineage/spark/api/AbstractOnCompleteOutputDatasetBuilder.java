/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * Extended version of output dataset builder {@link AbstractQueryPlanOutputDatasetBuilder} which is
 * only defined at the end of action being called. This is mainly used for LogicalPlan nodes which
 * create new datasets and it only makes sense to extract dataset after job is run.
 *
 * @param <P>
 */
public abstract class AbstractOnCompleteOutputDatasetBuilder<P extends LogicalPlan>
    extends AbstractQueryPlanOutputDatasetBuilder<P> {

  public AbstractOnCompleteOutputDatasetBuilder(
      OpenLineageContext context, boolean searchDependencies) {
    super(context, searchDependencies);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return (event instanceof SparkListenerSQLExecutionEnd || event instanceof SparkListenerJobEnd);
  }
}
