/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

public class CatalogUtils3 {

  private static List<RelationHandler> relationHandlers = getRelationHandlers();

  private static List<CatalogHandler> getHandlers(OpenLineageContext context) {
    List<CatalogHandler> commonHandlers =
        Arrays.asList(
            new DeltaHandler(context),
            new DatabricksDeltaHandler(context),
            new DatabricksUnityV2Handler(context),
            new GravitinoHandler(context),
            new V2SessionCatalogHandler());
    List<CatalogHandler> handlers = new LinkedList<>(commonHandlers);
    if (GravitinoInfoProviderImpl.getInstance().useGravitinoIdentifier()) {
      handlers.add(new GravitinoIcebergHandler(context));
      handlers.add(new GravitinoJDBCHandler(context));
    } else {
      handlers.add(new IcebergHandler(context));
      handlers.add(new JdbcHandler(context));
    }
    return handlers.stream().filter(CatalogHandler::hasClasses).collect(Collectors.toList());
  }

  private static List<RelationHandler> getRelationHandlers() {
    List<RelationHandler> handlers = Arrays.asList(new CosmosHandler());
    return handlers.stream().filter(RelationHandler::hasClasses).collect(Collectors.toList());
  }

  public static DatasetIdentifier getDatasetIdentifier(
      OpenLineageContext context,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    return getDatasetIdentifier(context, catalog, identifier, properties, getHandlers(context));
  }

  public static DatasetIdentifier getDatasetIdentifier(
      OpenLineageContext context,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties,
      List<CatalogHandler> handlers) {

    return handlers.stream()
        .filter(handler -> handler.isClass(catalog))
        .filter(handler -> context.getSparkSession().isPresent())
        .map(
            handler ->
                handler.getDatasetIdentifier(
                    context.getSparkSession().get(), catalog, identifier, properties))
        .findAny()
        .orElseThrow(
            () ->
                new UnsupportedCatalogException(
                    String.format(
                        "Cannot extract dataset for catalog=%s",
                        catalog.getClass().getCanonicalName())));
  }

  public static Optional<CatalogHandler> getCatalogHandler(
      OpenLineageContext context, TableCatalog catalog) {
    return getHandlers(context).stream().filter(handler -> handler.isClass(catalog)).findAny();
  }

  public static DatasetIdentifier getDatasetIdentifierFromRelation(DataSourceV2Relation relation) {
    return getDatasetIdentifierFromRelation(relation, relationHandlers);
  }

  public static DatasetIdentifier getDatasetIdentifierFromRelation(
      DataSourceV2Relation relation, List<RelationHandler> relationHandlers) {
    return relationHandlers.stream()
        .filter(handler -> handler.isClass(relation))
        .map(handler -> handler.getDatasetIdentifier(relation))
        .findAny()
        .orElseThrow(
            () ->
                new UnsupportedCatalogException(
                    String.format(
                        "Cannot extract dataset from relation=%s relationClass=%s",
                        relation.simpleString(5), relation.getClass().getCanonicalName())));
  }

  public static void addStorageAndCatalogFacets(
      OpenLineageContext context,
      TableCatalog catalog,
      Map<String, String> properties,
      DatasetCompositeFacetsBuilder builder) {
    CatalogUtils3.getStorageDatasetFacet(context, catalog, properties)
        .map(storageDatasetFacet -> builder.getFacets().storage(storageDatasetFacet));
    CatalogUtils3.getCatalogDatasetFacet(context, catalog, properties)
        .ifPresent(
            catalogDatasetFacet -> {
              builder.getFacets().catalog(catalogDatasetFacet.getCatalogDatasetFacet());
              catalogDatasetFacet
                  .getAdditionalFacets()
                  .forEach((k, v) -> builder.getFacets().put(k, v));
            });
  }

  public static Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      OpenLineageContext context, TableCatalog catalog, Map<String, String> properties) {
    Optional<CatalogHandler> catalogHandler = getCatalogHandler(context, catalog);
    return catalogHandler.isPresent()
        ? catalogHandler.get().getStorageDatasetFacet(properties)
        : Optional.empty();
  }

  public static Optional<CatalogHandler.CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      OpenLineageContext context, TableCatalog catalog, Map<String, String> properties) {
    Optional<CatalogHandler> catalogHandler = getCatalogHandler(context, catalog);
    return catalogHandler.isPresent()
        ? catalogHandler.get().getCatalogDatasetFacet(catalog, properties)
        : Optional.empty();
  }

  public static Optional<String> getDatasetVersion(
      OpenLineageContext context,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    Optional<CatalogHandler> catalogHandler = getCatalogHandler(context, catalog);
    return catalogHandler.isPresent()
        ? catalogHandler.get().getDatasetVersion(catalog, identifier, properties)
        : Optional.empty();
  }
}
