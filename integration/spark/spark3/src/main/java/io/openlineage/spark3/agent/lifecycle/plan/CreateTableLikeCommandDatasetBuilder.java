/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractOnCompleteOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;

/**
 * Dataset builder that matches an {@link CreateTableLikeCommand} and extracts the output {@link
 * OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateTableLikeCommandDatasetBuilder
    extends AbstractOnCompleteOutputDatasetBuilder<CreateTableLikeCommand> {

  public CreateTableLikeCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof CreateTableLikeCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(CreateTableLikeCommand command) {

    return context
        .getSparkSession()
        .map(
            session -> {
              SessionCatalog catalog = session.sessionState().catalog();

              CatalogTable source =
                  catalog.getTempViewOrPermanentTableMetadata(command.sourceTable());
              URI defaultLocation = catalog.defaultTablePath(command.targetTable());

              URI location =
                  ScalaConversionUtils.<URI>asJavaOptional(command.fileFormat().locationUri())
                      .orElse(defaultLocation);
              DatasetIdentifier di = PathUtils.fromURI(location);

              return Collections.singletonList(
                  outputDataset()
                      .sparkDatasetBuilder()
                      .dataset(di)
                      .schema(source.schema())
                      .catalog(source.identifier())
                      .lifecycleStateChange(
                          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE)
                      .build());
            })
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<String> jobNameSuffix(CreateTableLikeCommand command) {
    return Optional.ofNullable(command.targetTable()).map(TableIdentifier::identifier);
  }
}
