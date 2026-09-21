/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

public class CatalogUtils {

  /**
   * {@code hasClasses()} probes the classloader, and a miss costs a thrown {@link
   * ClassNotFoundException} with its stack trace. The answer cannot change within a JVM, so it is
   * memoized per handler class - the handler instances themselves are rebuilt per call, since they
   * capture the context.
   */
  private static final Map<Class<?>, Boolean> HAS_CLASSES = new ConcurrentHashMap<>();

  /** Declared after {@code HAS_CLASSES}, which its initializer filters through. */
  private static final List<RelationHandler> SHARED_RELATION_HANDLERS = getSharedRelationHandlers();

  private static boolean hasClasses(RelationHandler handler) {
    return HAS_CLASSES.computeIfAbsent(handler.getClass(), k -> handler.hasClasses());
  }

  private static List<CatalogHandler> getHandlers(OpenLineageContext context) {
    return context.getDatasetBuilderFactory().getCatalogHandlers(context).stream()
        .filter(CatalogHandler::hasClasses)
        .collect(Collectors.toList());
  }

  private static List<RelationHandler> getSharedRelationHandlers() {
    List<RelationHandler> handlers = Arrays.asList(new CosmosHandler());
    return handlers.stream().filter(CatalogUtils::hasClasses).collect(Collectors.toList());
  }

  private static List<RelationHandler> getRelationHandlers(OpenLineageContext context) {
    return Stream.concat(
            SHARED_RELATION_HANDLERS.stream(),
            context.getDatasetBuilderFactory().getRelationHandlers(context).stream()
                .filter(CatalogUtils::hasClasses))
        .collect(Collectors.toList());
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

  /**
   * @deprecated Resolves the relation against the version-independent handlers only. Use {@link
   *     #getDatasetIdentifierFromRelation(OpenLineageContext, DataSourceV2Relation)}, which also
   *     consults the handlers contributed by the running Spark version.
   */
  @Deprecated
  public static DatasetIdentifier getDatasetIdentifierFromRelation(DataSourceV2Relation relation) {
    return getDatasetIdentifierFromRelation(relation, SHARED_RELATION_HANDLERS);
  }

  public static DatasetIdentifier getDatasetIdentifierFromRelation(
      OpenLineageContext context, DataSourceV2Relation relation) {
    return getDatasetIdentifierFromRelation(relation, getRelationHandlers(context));
  }

  /**
   * The catalog that owns the relation's table, according to the first {@link RelationHandler} that
   * recognises the relation. Empty when no handler matches or none can recover a catalog.
   */
  public static Optional<RelationHandler.OwningCatalog> getOwningCatalogFromRelation(
      OpenLineageContext context, DataSourceV2Relation relation) {
    return getRelationHandlers(context).stream()
        .filter(handler -> handler.isClass(relation))
        .findAny()
        .flatMap(handler -> handler.getOwningCatalog(relation));
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
    CatalogUtils.getStorageDatasetFacet(context, catalog, properties)
        .map(storageDatasetFacet -> builder.getFacets().storage(storageDatasetFacet));
    CatalogUtils.getCatalogDatasetFacet(context, catalog, properties)
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
