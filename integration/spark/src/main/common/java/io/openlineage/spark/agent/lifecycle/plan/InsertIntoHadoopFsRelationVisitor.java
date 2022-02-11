package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

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
    Map<String, OpenLineage.DatasetFacet> facets =
        (SaveMode.Overwrite == command.mode())
            ? Collections.singletonMap(
                "tableStateChange", new TableStateChangeFacet(StateChange.OVERWRITE))
            : Collections.emptyMap();

    return Collections.singletonList(
        outputDataset()
            .getDataset(
                PathUtils.fromURI(command.outputPath().toUri(), "file"),
                command.query().schema(),
                facets));
  }
}
