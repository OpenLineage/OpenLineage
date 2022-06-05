/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;

import java.util.Collections;
import java.util.List;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHiveTable} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHiveTableVisitor
    extends QueryPlanVisitor<InsertIntoHiveTable, OpenLineage.OutputDataset> {

  public InsertIntoHiveTableVisitor(OpenLineageContext context) {
    super(context);
  }

  public static boolean hasHiveClasses() {
    try {
      InsertIntoHiveTableVisitor.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.hive.execution.InsertIntoHiveTable");
      return true;
    } catch (ClassNotFoundException e) {
      // ignore
    }
    return false;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoHiveTable cmd = (InsertIntoHiveTable) x;
    CatalogTable table = cmd.table();

    OpenLineage.OutputDataset outputDataset;
    if (cmd.overwrite()) {
      outputDataset =
          outputDataset()
              .getDataset(
                  PathUtils.fromCatalogTable(table),
                  table.schema(),
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    } else {
      outputDataset = outputDataset().getDataset(PathUtils.fromCatalogTable(table), table.schema());
    }

    return Collections.singletonList(outputDataset);
  }
}
