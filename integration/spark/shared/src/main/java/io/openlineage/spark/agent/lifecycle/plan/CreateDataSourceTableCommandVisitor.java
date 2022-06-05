/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand;

import java.util.Collections;
import java.util.List;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateDataSourceTableCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class CreateDataSourceTableCommandVisitor
    extends QueryPlanVisitor<CreateDataSourceTableCommand, OpenLineage.OutputDataset> {

  public CreateDataSourceTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    CreateDataSourceTableCommand command = (CreateDataSourceTableCommand) x;
    CatalogTable catalogTable = command.table();

    return Collections.singletonList(
        outputDataset()
            .getDataset(
                PathUtils.fromCatalogTable(catalogTable),
                catalogTable.schema(),
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE));
  }
}
