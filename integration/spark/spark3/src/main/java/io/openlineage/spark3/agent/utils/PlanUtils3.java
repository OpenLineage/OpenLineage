/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Utility functions for traversing a {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} for Spark 3.
 */
@Slf4j
public class PlanUtils3 {

  public static Optional<DatasetIdentifier> getDatasetIdentifier(
      OpenLineageContext context, DataSourceV2Relation relation) {

    if (relation.identifier() == null || relation.identifier().isEmpty()) {
      // Since identifier is null, short circuit and check if we can get the dataset identifer
      // from the relation itself.
      return getDatasetIdentifierFromRelation(relation);
    }
    return Optional.of(relation)
        .filter(r -> r.identifier() != null)
        .filter(r -> r.identifier().isDefined())
        .filter(r -> r.catalog() != null)
        .filter(r -> r.catalog().isDefined())
        .filter(r -> r.catalog().get() instanceof TableCatalog)
        .flatMap(
            r ->
                PlanUtils3.getDatasetIdentifier(
                    context,
                    (TableCatalog) r.catalog().get(),
                    r.identifier().get(),
                    r.table().properties()));
  }

  // Ensure that resources like this SparkSession object are closed after use -> we
  // don't want to close SparkSession
  @SuppressWarnings("PMD")
  public static Optional<DatasetIdentifier> getDatasetIdentifier(
      OpenLineageContext context,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {

    if (!context.getSparkSession().isPresent()) {
      throw new IllegalArgumentException("SparkSession cannot be empty");
    }

    try {
      return (Optional.of(
          CatalogUtils3.getDatasetIdentifier(context, catalog, identifier, properties)));
    } catch (UnsupportedCatalogException ex) {
      log.error(String.format("Catalog %s is unsupported", ex.getMessage()), ex);
      return Optional.empty();
    }
  }

  private static Optional<DatasetIdentifier> getDatasetIdentifierFromRelation(
      DataSourceV2Relation relation) {

    try {
      return (Optional.of(CatalogUtils3.getDatasetIdentifierFromRelation(relation)));
    } catch (UnsupportedCatalogException ex) {
      log.error(
          String.format("Catalog %s is unsupported", ex.getMessage()),
          ex); // update this if change the exception thrown in catalogutils
      return Optional.empty();
    }
  }

  public static <D extends OpenLineage.Dataset> List<D> fromDataSourceV2Relation(
      DatasetFactory<D> datasetFactory, OpenLineageContext context, DataSourceV2Relation relation) {
    return fromDataSourceV2Relation(
        datasetFactory, context, relation, context.getOpenLineage().newDatasetFacetsBuilder());
  }

  public static <D extends OpenLineage.Dataset> List<D> fromDataSourceV2Relation(
      DatasetFactory<D> datasetFactory,
      OpenLineageContext context,
      DataSourceV2Relation relation,
      OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder) {

    OpenLineage openLineage = context.getOpenLineage();

    Optional<DatasetIdentifier> di;
    // Get identifier for dataset, or return empty list
    if (relation.identifier().isEmpty()) {
      log.warn("Couldn't find identifier for dataset in plan {}", relation);
      di = PlanUtils3.getDatasetIdentifier(context, relation);
      if (!di.isPresent()) {
        return Collections.emptyList();
      }
    } else {
      Identifier identifier = relation.identifier().get();

      // Get catalog for dataset, or return empty list
      if (relation.catalog().isEmpty() || !(relation.catalog().get() instanceof TableCatalog)) {
        log.warn("Couldn't find catalog for dataset in plan " + relation);
        return Collections.emptyList();
      }
      TableCatalog tableCatalog = (TableCatalog) relation.catalog().get();

      Map<String, String> tableProperties = relation.table().properties();
      di = PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

      if (!di.isPresent()) {
        return Collections.emptyList();
      }

      CatalogUtils3.getStorageDatasetFacet(context, tableCatalog, tableProperties)
          .map(storageDatasetFacet -> datasetFacetsBuilder.storage(storageDatasetFacet));
    }

    datasetFacetsBuilder
        .schema(PlanUtils.schemaFacet(openLineage, relation.schema()))
        .dataSource(PlanUtils.datasourceFacet(openLineage, di.get().getNamespace()));

    return Collections.singletonList(
        datasetFactory.getDataset(
            di.get().getName(), di.get().getNamespace(), datasetFacetsBuilder.build()));
  }
}
