/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark2.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.agent.util.DatasetIdentifier;
import io.openlineage.spark.shared.agent.util.PathUtils;
import io.openlineage.spark.shared.api.OpenLineageContext;
import io.openlineage.spark.shared.api.QueryPlanVisitor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableLikeCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateTableLikeCommandVisitor
    extends QueryPlanVisitor<CreateTableLikeCommand, OpenLineage.OutputDataset> {

  public CreateTableLikeCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    CreateTableLikeCommand command = (CreateTableLikeCommand) x;
    return context
        .getSparkSession()
        .map(
            session -> {
              SessionCatalog catalog = session.sessionState().catalog();

              CatalogTable source =
                  catalog.getTempViewOrPermanentTableMetadata(command.sourceTable());
              URI location;
              if (command.location().isEmpty()) {
                location = catalog.defaultTablePath(command.targetTable());
              } else {
                try {
                  location = new URI(command.location().get());
                } catch (URISyntaxException e) {
                  log.warn("Invalid URI found for command location {}", command.location().get());
                  return Collections.<OpenLineage.OutputDataset>emptyList();
                }
              }

              DatasetIdentifier di = PathUtils.fromURI(location, "file");
              return Collections.singletonList(
                  outputDataset()
                      .getDataset(
                          di,
                          source.schema(),
                          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                              .CREATE));
            })
        .orElse(Collections.emptyList());
  }
}
