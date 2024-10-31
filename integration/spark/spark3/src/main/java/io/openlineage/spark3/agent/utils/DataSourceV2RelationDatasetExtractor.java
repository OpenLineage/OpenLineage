/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
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

@Slf4j
public class DataSourceV2RelationDatasetExtractor {

  public static <D extends OpenLineage.Dataset> List<D> extract(
      DatasetFactory<D> datasetFactory, OpenLineageContext context, DataSourceV2Relation relation) {
    return extract(datasetFactory, context, relation, datasetFactory.createCompositeFacetBuilder());
  }

  public static <D extends OpenLineage.Dataset> List<D> extract(
      DatasetFactory<D> datasetFactory,
      OpenLineageContext context,
      DataSourceV2Relation relation,
      DatasetCompositeFacetsBuilder datasetFacetsBuilder) {

    OpenLineage openLineage = context.getOpenLineage();
    Optional<DatasetIdentifier> di = getDatasetIdentifierExtended(context, relation);
    return di.map(
            identifier -> {
              if (ExtensionDataSourceV2Utils.hasExtensionLineage(relation)) {
                ExtensionDataSourceV2Utils.loadBuilder(openLineage, datasetFacetsBuilder, relation);
              } else {
                TableCatalog tableCatalog = (TableCatalog) relation.catalog().get();

                Map<String, String> tableProperties = relation.table().properties();
                CatalogUtils3.getStorageDatasetFacet(context, tableCatalog, tableProperties)
                    .map(
                        storageDatasetFacet ->
                            datasetFacetsBuilder.getFacets().storage(storageDatasetFacet));
              }
              datasetFacetsBuilder
                  .getFacets()
                  .schema(PlanUtils.schemaFacet(openLineage, relation.schema()))
                  .dataSource(PlanUtils.datasourceFacet(openLineage, identifier.getNamespace()));

              return Collections.singletonList(
                  datasetFactory.getDataset(identifier, datasetFacetsBuilder));
            })
        .orElse(Collections.emptyList());
  }

  public static Optional<DatasetIdentifier> getDatasetIdentifier(
      OpenLineageContext context, DataSourceV2Relation relation) {

    if (relation.identifier() == null || relation.identifier().isEmpty()) {
      // Since identifier is null, short circuit and check if we can get the dataset identifier
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

  public static Optional<DatasetIdentifier> getDatasetIdentifierExtended(
      OpenLineageContext context, DataSourceV2Relation relation) {
    // Check if the dataset has extension lineage
    if (ExtensionDataSourceV2Utils.hasExtensionLineage(relation)) {
      return Optional.of(ExtensionDataSourceV2Utils.getDatasetIdentifier(relation));
    }

    // Check if the relation identifier is empty
    if (relation.identifier().isEmpty()) {
      log.warn("Couldn't find identifier for dataset in plan {}", relation);
      return getDatasetIdentifier(context, relation);
    }

    // Check if the catalog is present and is an instance of TableCatalog
    if (relation.catalog().isEmpty() || !(relation.catalog().get() instanceof TableCatalog)) {
      log.warn("Couldn't find catalog for dataset in plan {}", relation);
      return Optional.empty();
    }

    Identifier identifier = relation.identifier().get();
    TableCatalog tableCatalog = (TableCatalog) relation.catalog().get();
    Map<String, String> tableProperties = relation.table().properties();

    // Get the dataset identifier
    return PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);
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
}
