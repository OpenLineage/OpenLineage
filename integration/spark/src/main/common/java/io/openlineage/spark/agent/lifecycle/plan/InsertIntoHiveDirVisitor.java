package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHiveDirCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHiveDirVisitor
    extends QueryPlanVisitor<InsertIntoHiveDirCommand, OpenLineage.Dataset> {

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    InsertIntoHiveDirCommand cmd = (InsertIntoHiveDirCommand) x;
    Optional<URI> optionalUri = ScalaConversionUtils.asJavaOptional(cmd.storage().locationUri());
    return optionalUri
        .map(
            uri ->
                Collections.singletonList(
                    PlanUtils.getDataset(PathUtils.fromURI(uri, "file"), cmd.query().schema())))
        .orElse(Collections.emptyList());
  }
}
