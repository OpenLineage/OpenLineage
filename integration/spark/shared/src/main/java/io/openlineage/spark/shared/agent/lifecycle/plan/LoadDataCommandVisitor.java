/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.agent.util.PathUtils;
import io.openlineage.spark.shared.api.OpenLineageContext;
import io.openlineage.spark.shared.api.QueryPlanVisitor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.LoadDataCommand;

import java.util.Collections;
import java.util.List;

/**
 * {@link LogicalPlan} visitor that matches an {@link LoadDataCommandVisitor} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class LoadDataCommandVisitor
    extends QueryPlanVisitor<LoadDataCommand, OpenLineage.OutputDataset> {

  public LoadDataCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    LoadDataCommand command = (LoadDataCommand) x;
    return catalogTableFor(command.table())
        .map(
            table ->
                Collections.singletonList(
                    outputDataset().getDataset(PathUtils.fromCatalogTable(table), table.schema())))
        .orElseGet(Collections::emptyList);
  }
}
