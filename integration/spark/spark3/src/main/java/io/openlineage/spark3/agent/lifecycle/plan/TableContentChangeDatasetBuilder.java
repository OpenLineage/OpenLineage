/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.IcebergHandler;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
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
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;

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
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
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

    final OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        context.getOpenLineage().newDatasetFacetsBuilder();
    if (includeOverwriteFacet) {
      datasetFacetsBuilder.lifecycleStateChange(
          context
              .getOpenLineage()
              .newLifecycleStateChangeDatasetFacet(
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
                  null));
    }

    // FIXME: Use 'castToDataSourceV2Relation()' to safely cast 'DataSourceV2ScanRelation' to
    // 'DataSourceV2Relation'. We are unsure of the logic plan structure that would cause a
    // 'ClassCastException' to be thrown; therefore, to get meaningful insight we also log the
    // logical plan when the relation is of the type 'DataSourceV2ScanRelation'.
    final DataSourceV2Relation returnTable =
        (table instanceof DataSourceV2ScanRelation)
            ? castToDataSourceV2Relation(x, table)
            : (DataSourceV2Relation) table;
    if (includeDatasetVersion(event)) {
      DatasetVersionDatasetFacetUtils.includeDatasetVersion(
          context, datasetFacetsBuilder, returnTable);
    }

    return PlanUtils3.fromDataSourceV2Relation(
        outputDataset(), context, returnTable, datasetFacetsBuilder);
  }

  private DataSourceV2Relation castToDataSourceV2Relation(LogicalPlan x, NamedRelation table) {
    // Log warning, then return the underlying relation from the scan relation to avoid
    // 'ClassCastException'.
    log.warn(
        "The relation '{}' is of an invalid type 'DataSourceV2ScanRelation', and should not be "
            + "handled as an output relation. The cast operation will be applied, but the logical "
            + "plan associated with the relation may contain an unexpected structure: {}",
        table.name(),
        x);
    return ((DataSourceV2ScanRelation) table).relation();
  }
}
