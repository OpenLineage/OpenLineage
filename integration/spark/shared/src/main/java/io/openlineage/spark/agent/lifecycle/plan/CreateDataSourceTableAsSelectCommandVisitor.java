/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateDataSourceTableAsSelectCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class CreateDataSourceTableAsSelectCommandVisitor
    extends QueryPlanVisitor<CreateDataSourceTableAsSelectCommand, OpenLineage.OutputDataset> {

  public CreateDataSourceTableAsSelectCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    CreateDataSourceTableAsSelectCommand command = (CreateDataSourceTableAsSelectCommand) x;
    CatalogTable catalogTable = command.table();

    // Sometimes the schema is missing from the catalog (e.g., when the table doesn't yet exist)
    StructType schema = catalogTable.schema();
    if (schema.fields().length == 0 && command.query().schema().fields().length > 0) {
      schema = command.query().schema();
    }
    return Collections.singletonList(
        outputDataset()
            .getDataset(
                PathUtils.fromCatalogTable(catalogTable),
                schema,
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE));
  }
}
