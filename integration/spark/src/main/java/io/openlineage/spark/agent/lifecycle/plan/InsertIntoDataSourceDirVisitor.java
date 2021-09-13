package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
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
    extends QueryPlanVisitor<InsertIntoDataSourceDirCommand> {

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    InsertIntoDataSourceDirCommand command = (InsertIntoDataSourceDirCommand) x;
    // URI is required by the InsertIntoDataSourceDirCommand
    URI outputPath = command.storage().locationUri().get();
    if (outputPath.getScheme() == null) {
      outputPath = URI.create("file://" + outputPath);
    }
    String namespace = PlanUtils.namespaceUri(outputPath);
    return Collections.singletonList(
        PlanUtils.getDataset(
            outputPath.getPath(), namespace, PlanUtils.datasetFacet(command.schema(), namespace)));
  }
}
