package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.PlanUtils.datasourceFacet;
import static io.openlineage.spark.agent.util.PlanUtils.schemaFacet;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.PreviousTableNameFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableRenameCommand;

@Slf4j
public class AlterTableRenameCommandVisitor
    extends QueryPlanVisitor<AlterTableRenameCommand, OpenLineage.Dataset> {

  private final SparkSession sparkSession;

  public AlterTableRenameCommandVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @SneakyThrows
  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    SessionCatalog sessionCatalog = sparkSession.sessionState().catalog();
    CatalogTable table;
    try {
      table = sessionCatalog.getTableMetadata(((AlterTableRenameCommand) x).newName());
    } catch (NoSuchTableException e) {
      log.info("NoSuchTableException caught");
      // apply method called before altering table - do not send an event
      return Collections.emptyList();
    }

    DatasetIdentifier di = PathUtils.fromCatalogTable(table);

    AlterTableRenameCommand alterTableRenameCommand = (AlterTableRenameCommand) x;
    String previousPath =
        di.getName()
            .replace(
                alterTableRenameCommand.newName().table(),
                alterTableRenameCommand.oldName().table());

    return Collections.singletonList(
        PlanUtils.getDataset(
            di,
            new OpenLineage.DatasetFacetsBuilder()
                .schema(schemaFacet(table.schema()))
                .dataSource(datasourceFacet(di.getNamespace()))
                .put("previousTableName", new PreviousTableNameFacet(previousPath, di.getName()))
                .build()));
  }
}
