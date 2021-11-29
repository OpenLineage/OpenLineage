package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDataSourceDirCommand} and extracts
 * the output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoDataSourceDirVisitor
    extends QueryPlanVisitor<InsertIntoDataSourceDirCommand, OpenLineage.Dataset> {

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    InsertIntoDataSourceDirCommand command = (InsertIntoDataSourceDirCommand) x;
    // URI is required by the InsertIntoDataSourceDirCommand
    DatasetIdentifier di = PathUtils.fromURI(command.storage().locationUri().get(), "file");
    return Collections.singletonList(PlanUtils.getDataset(di, command.schema()));
  }
}
