package io.openlineage.spark2.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableLikeCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
@Slf4j
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
    URI location =
        command
            .location()
            .getOrElse(
                ScalaConversionUtils.toScalaFn(
                    () -> catalog.defaultTablePath(command.targetTable())));

    DatasetIdentifier di = PathUtils.fromURI(location, "file");
    return Collections.singletonList(PlanUtils.getDataset(di, source.schema()));
  }
}
