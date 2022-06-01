/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.agent.util.DatasetIdentifier;
import io.openlineage.spark.shared.agent.util.PathUtils;
import io.openlineage.spark.shared.api.OpenLineageContext;
import io.openlineage.spark.shared.api.QueryPlanVisitor;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;

import java.util.Collections;
import java.util.List;

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
    DatasetIdentifier di = PathUtils.fromURI(command.storage().locationUri().get(), "file");

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

    return Collections.singletonList(outputDataset);
  }
}
