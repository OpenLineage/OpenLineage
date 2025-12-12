/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.gvfs.GVFSUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDataSourceDirCommand} and extracts
 * the output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoDataSourceDirVisitor
    extends QueryPlanVisitor<InsertIntoDataSourceDirCommand, OpenLineage.OutputDataset> {

  public InsertIntoDataSourceDirVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoDataSourceDirCommand command = (InsertIntoDataSourceDirCommand) x;
    // URI is required by the InsertIntoDataSourceDirCommand
    URI location = command.storage().locationUri().get();
    DatasetIdentifier di = PathUtils.fromURI(location);

    OpenLineage.OutputDataset outputDataset;
    if (command.overwrite()) {
      outputDataset =
          outputDataset()
              .getDataset(
                  di,
                  command.schema(),
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    } else {
      outputDataset = outputDataset().getDataset(di, command.schema());
    }

    if (GVFSUtils.isGVFS(location)) {
      outputDataset = GVFSUtils.createGVFSOutputDataset(context.getOpenLineage(), outputDataset, location);
    }

    return Collections.singletonList(outputDataset);
  }
}
