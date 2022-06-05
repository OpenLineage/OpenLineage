/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

import java.util.Collections;
import java.util.List;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHadoopFsRelationCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHadoopFsRelationVisitor
    extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, OpenLineage.OutputDataset> {

  public InsertIntoHadoopFsRelationVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) x;

    DatasetIdentifier di = PathUtils.fromURI(command.outputPath().toUri(), "file");
    OpenLineage.OutputDataset outputDataset;
    if (SaveMode.Overwrite == command.mode()) {
      outputDataset =
          outputDataset()
              .getDataset(
                  di,
                  command.query().schema(),
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    } else {
      outputDataset = outputDataset().getDataset(di, command.query().schema());
    }

    return Collections.singletonList(outputDataset);
  }
}
