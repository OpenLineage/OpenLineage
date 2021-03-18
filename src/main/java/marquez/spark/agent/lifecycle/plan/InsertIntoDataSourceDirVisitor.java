package marquez.spark.agent.lifecycle.plan;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import marquez.spark.agent.client.LineageEvent.Dataset;
import marquez.spark.agent.facets.OutputStatisticsFacet;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDataSourceDirCommand} and extracts
 * the output {@link Dataset} being written.
 */
public class InsertIntoDataSourceDirVisitor
    extends AbstractPartialFunction<LogicalPlan, List<Dataset>> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof InsertIntoDataSourceDirCommand;
  }

  @Override
  public List<Dataset> apply(LogicalPlan x) {
    InsertIntoDataSourceDirCommand command = (InsertIntoDataSourceDirCommand) x;
    OutputStatisticsFacet outputStats = PlanUtils.getOutputStats(command.metrics());
    // URI is required by the InsertIntoDataSourceDirCommand
    URI outputPath = command.storage().locationUri().get();
    String namespace = PlanUtils.namespaceUri(outputPath);
    return Collections.singletonList(
        PlanUtils.getDataset(
            outputPath,
            namespace,
            PlanUtils.datasetFacet(command.schema(), namespace, outputStats)));
  }
}
