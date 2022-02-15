/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.PreviousTableNameFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableRenameCommand;

@Slf4j
public class AlterTableRenameCommandVisitor
    extends QueryPlanVisitor<AlterTableRenameCommand, OpenLineage.OutputDataset> {

  public AlterTableRenameCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @SneakyThrows
  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    Optional<CatalogTable> tableOpt = catalogTableFor(((AlterTableRenameCommand) x).newName());
    if (!tableOpt.isPresent()) {
      return Collections.emptyList();
    }
    CatalogTable table = tableOpt.get();

    DatasetIdentifier di = PathUtils.fromCatalogTable(table);

    AlterTableRenameCommand alterTableRenameCommand = (AlterTableRenameCommand) x;
    String previousPath =
        di.getName()
            .replace(
                alterTableRenameCommand.newName().table(),
                alterTableRenameCommand.oldName().table());

    DatasetFactory<OpenLineage.OutputDataset> factory = outputDataset();
    return Collections.singletonList(
        factory.getDataset(
            di,
            new OpenLineage.DatasetFacetsBuilder()
                .schema(PlanUtils.schemaFacet(context.getOpenLineage(), table.schema()))
                .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), di.getNamespace()))
                .put("previousTableName", new PreviousTableNameFacet(previousPath, di.getName()))
                .build()));
  }
}
