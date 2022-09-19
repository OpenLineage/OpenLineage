/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.TruncateTableCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link TruncateTableCommand} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class TruncateTableCommandVisitor
    extends QueryPlanVisitor<TruncateTableCommand, OutputDataset> {

  public TruncateTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OutputDataset> apply(LogicalPlan x) {
    TruncateTableCommand command = (TruncateTableCommand) x;

    Optional<CatalogTable> tableOpt = catalogTableFor(command.tableName());
    if (tableOpt.isPresent()) {
      CatalogTable table = tableOpt.get();
      DatasetIdentifier datasetIdentifier = PathUtils.fromCatalogTable(table);

      DatasetFactory<OutputDataset> datasetFactory = outputDataset();
      return Collections.singletonList(
          datasetFactory.getDataset(
              datasetIdentifier,
              new OpenLineage.DatasetFacetsBuilder()
                  .schema(null)
                  .dataSource(
                      PlanUtils.datasourceFacet(
                          context.getOpenLineage(), datasetIdentifier.getNamespace()))
                  .lifecycleStateChange(
                      context
                          .getOpenLineage()
                          .newLifecycleStateChangeDatasetFacet(
                              OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                                  .TRUNCATE,
                              null))));

    } else {
      // table does not exist, cannot prepare an event
      return Collections.emptyList();
    }
  }
}
