/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.shared.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.IcebergHandler;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression;
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class TableContentChangeDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public TableContentChangeDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return (x instanceof OverwriteByExpression)
        || (x instanceof OverwritePartitionsDynamic)
        || (x instanceof DeleteFromTable)
        || (x instanceof UpdateTable)
        || (new IcebergHandler().hasClasses() && x instanceof ReplaceData)
        || (x instanceof MergeIntoTable)
        || (x instanceof InsertIntoStatement);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    NamedRelation table;
    boolean includeOverwriteFacet = false;

    // INSERT OVERWRITE TABLE SQL statement is translated into InsertIntoTable logical operator.
    if (x instanceof OverwriteByExpression) {
      table = ((OverwriteByExpression) x).table();
      includeOverwriteFacet = true;
    } else if (x instanceof InsertIntoStatement) {
      table = (NamedRelation) ((InsertIntoStatement) x).table();
      if (((InsertIntoStatement) x).overwrite()) {
        includeOverwriteFacet = true;
      }
    } else if (new IcebergHandler().hasClasses() && x instanceof ReplaceData) {
      // DELETE FROM on ICEBERG HAS START ELEMENT WITH ReplaceData AND COMPLETE ONE WITH
      // DeleteFromTable
      table = ((ReplaceData) x).table();
    } else if (x instanceof DeleteFromTable) {
      table = (NamedRelation) ((DeleteFromTable) x).table();
    } else if (x instanceof UpdateTable) {
      table = (NamedRelation) ((UpdateTable) x).table();
    } else if (x instanceof MergeIntoTable) {
      table = (NamedRelation) ((MergeIntoTable) x).targetTable();
    } else {
      table = ((OverwritePartitionsDynamic) x).table();
      includeOverwriteFacet = true;
    }

    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();
    if (includeOverwriteFacet) {
      datasetFacetsBuilder.lifecycleStateChange(
          context
              .getOpenLineage()
              .newLifecycleStateChangeDatasetFacet(
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
                  null));
    }

    DatasetVersionDatasetFacetUtils.includeDatasetVersion(
        context, datasetFacetsBuilder, (DataSourceV2Relation) table);
    return PlanUtils3.fromDataSourceV2Relation(
        outputDataset(), context, (DataSourceV2Relation) table, datasetFacetsBuilder);
  }
}
