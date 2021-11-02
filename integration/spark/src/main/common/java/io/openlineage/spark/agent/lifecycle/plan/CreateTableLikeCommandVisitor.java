package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableLikeCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class CreateTableLikeCommandVisitor
    extends QueryPlanVisitor<CreateTableLikeCommand, OpenLineage.Dataset> {

  private final SparkSession sparkSession;

  public CreateTableLikeCommandVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @SneakyThrows
  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    CreateTableLikeCommand command = (CreateTableLikeCommand) x;
    SessionCatalog catalog = sparkSession.sessionState().catalog();
    CatalogTable source = catalog.getTempViewOrPermanentTableMetadata(command.sourceTable());

    Path path = PlanUtils.getPath(source.location(), command.targetTable().identifier(), "");

    return Collections.singletonList(PlanUtils.getDataset(path.toUri(), source.schema()));
  }
}
