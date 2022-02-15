/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateTableCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableCommand} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class CreateTableCommandVisitor
    extends QueryPlanVisitor<CreateTableCommand, OpenLineage.OutputDataset> {

  public CreateTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    CreateTableCommand command = (CreateTableCommand) x;
    CatalogTable catalogTable = command.table();

    return Collections.singletonList(
        outputDataset()
            .getDataset(
                PathUtils.fromCatalogTable(catalogTable),
                catalogTable.schema(),
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE));
  }
}
