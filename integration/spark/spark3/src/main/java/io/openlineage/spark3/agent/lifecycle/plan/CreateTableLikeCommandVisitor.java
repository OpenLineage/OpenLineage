/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static java.util.Collections.singletonList;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

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
  public boolean isDefinedAt(SparkListenerEvent event) {
    return (event instanceof SparkListenerSQLExecutionEnd || event instanceof SparkListenerJobEnd);
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
              URI defaultLocation = catalog.defaultTablePath(command.targetTable());

              URI location =
                  ScalaConversionUtils.<URI>asJavaOptional(command.fileFormat().locationUri())
                      .orElse(defaultLocation);
              DatasetIdentifier di = PathUtils.fromURI(location);

              return singletonList(
                  outputDataset()
                      .getDataset(
                          di,
                          source.schema(),
                          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
                              .CREATE));
            })
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<String> jobNameSuffix(CreateTableLikeCommand command) {
    return Optional.ofNullable(command.targetTable()).map(TableIdentifier::identifier);
  }
}
