package openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHadoopFsRelationCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHadoopFsRelationVisitor
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof InsertIntoHadoopFsRelationCommand;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) x;
    URI outputPath = command.outputPath().toUri();
    String namespace = PlanUtils.namespaceUri(outputPath);
    return Collections.singletonList(
        PlanUtils.getDataset(
            outputPath.getPath(),
            namespace,
            PlanUtils.datasetFacet(command.query().schema(), namespace)));
  }
}
