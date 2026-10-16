/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;

/**
 * Dataset builder that matches an {@link InsertIntoDataSourceDirCommand} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoDataSourceDirDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<InsertIntoDataSourceDirCommand> {

  public InsertIntoDataSourceDirDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof InsertIntoDataSourceDirCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(InsertIntoDataSourceDirCommand command) {
    // URI is required by the InsertIntoDataSourceDirCommand
    SparkDatasetBuilder<OpenLineage.OutputDataset> builder =
        outputDataset()
            .sparkDatasetBuilder()
            .dataset(command.storage().locationUri().get())
            .schema(command.schema());
    if (command.overwrite()) {
      builder.lifecycleStateChange(
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    }
    return Collections.singletonList(builder.build());
  }
}
