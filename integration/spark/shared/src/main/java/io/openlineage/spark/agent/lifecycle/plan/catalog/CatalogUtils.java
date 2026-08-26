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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

public class CatalogUtils {

  private static List<RelationHandler> relationHandlers = getRelationHandlers();
  private static final ThreadLocal<EventCache> ACTIVE_EVENT_CACHE = new ThreadLocal<>();

  private static List<CatalogHandler> getHandlers(OpenLineageContext context) {
    Supplier<List<CatalogHandler>> loader =
        () ->
            context.getDatasetBuilderFactory().getCatalogHandlers(context).stream()
                .filter(CatalogHandler::hasClasses)
                .collect(Collectors.toList());
    EventCache cache = ACTIVE_EVENT_CACHE.get();
    return cache == null ? loader.get() : cache.getHandlers(context, loader);
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
    EventCache cache = ACTIVE_EVENT_CACHE.get();
    Supplier<Optional<CatalogHandler>> loader =
        () -> getHandlers(context).stream().filter(handler -> handler.isClass(catalog)).findAny();
    return cache == null ? loader.get() : cache.getCatalogHandler(context, catalog, loader);
  }

  /** Returns a new cache whose lifetime must be limited to one OpenLineage event build. */
  public static EventCache newEventCache() {
    return new EventCache();
  }

  /** Loads a Spark table at most once for the active event. */
  public static Table loadTable(TableCatalog catalog, Identifier identifier)
      throws NoSuchTableException {
    return loadTable(catalog, identifier, "spark-table", () -> catalog.loadTable(identifier));
  }

  /**
   * Loads an arbitrary catalog representation at most once for the active event. The operation name
   * distinguishes multiple representations of the same table, such as Spark and Iceberg tables.
   */
  @SneakyThrows
  public static <T> T loadTable(
      TableCatalog catalog, Identifier identifier, String operation, Callable<T> loader) {
    EventCache cache = ACTIVE_EVENT_CACHE.get();
    return cache == null ? loader.call() : cache.load(catalog, identifier, operation, loader);
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

  /** Per-event handler and loaded-table cache. */
  public static final class EventCache {
    private final Map<IdentityKey<OpenLineageContext>, List<CatalogHandler>> handlers =
        new ConcurrentHashMap<>();
    private final Map<HandlerKey, Optional<CatalogHandler>> catalogHandlers =
        new ConcurrentHashMap<>();
    private final Map<TableLoadKey, Future<Object>> loadedTables = new ConcurrentHashMap<>();

    /** Executes one event-build phase with this cache active on the calling thread. */
    @SneakyThrows
    public <T> T call(Callable<T> callable) {
      EventCache previous = ACTIVE_EVENT_CACHE.get();
      ACTIVE_EVENT_CACHE.set(this);
      try {
        return callable.call();
      } finally {
        if (previous == null) {
          ACTIVE_EVENT_CACHE.remove();
        } else {
          ACTIVE_EVENT_CACHE.set(previous);
        }
      }
    }

    private List<CatalogHandler> getHandlers(
        OpenLineageContext context, Supplier<List<CatalogHandler>> loader) {
      return handlers.computeIfAbsent(new IdentityKey<>(context), ignored -> loader.get());
    }

    private Optional<CatalogHandler> getCatalogHandler(
        OpenLineageContext context,
        TableCatalog catalog,
        Supplier<Optional<CatalogHandler>> loader) {
      return catalogHandlers.computeIfAbsent(
          new HandlerKey(context, catalog), ignored -> loader.get());
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    private <T> T load(
        TableCatalog catalog, Identifier identifier, String operation, Callable<T> loader) {
      TableLoadKey key = new TableLoadKey(catalog, identifier, operation);
      Future<Object> future = loadedTables.get(key);
      if (future == null) {
        FutureTask<Object> newFuture = new FutureTask<>(loader::call);
        future = loadedTables.putIfAbsent(key, newFuture);
        if (future == null) {
          future = newFuture;
          newFuture.run();
        }
      }

      try {
        return (T) future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }
  }

  private static final class IdentityKey<T> {
    private final T value;

    private IdentityKey(T value) {
      this.value = value;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // cache keys intentionally use identity
    public boolean equals(Object other) {
      return other instanceof IdentityKey && value == ((IdentityKey<?>) other).value;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(value);
    }
  }

  private static final class HandlerKey {
    private final OpenLineageContext context;
    private final TableCatalog catalog;

    private HandlerKey(OpenLineageContext context, TableCatalog catalog) {
      this.context = context;
      this.catalog = catalog;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // cache keys intentionally use identity
    public boolean equals(Object other) {
      if (!(other instanceof HandlerKey)) {
        return false;
      }
      HandlerKey that = (HandlerKey) other;
      return context == that.context && catalog == that.catalog;
    }

    @Override
    public int hashCode() {
      return 31 * System.identityHashCode(context) + System.identityHashCode(catalog);
    }
  }

  private static final class TableLoadKey {
    private final TableCatalog catalog;
    private final String identifier;
    private final String operation;

    private TableLoadKey(TableCatalog catalog, Identifier identifier, String operation) {
      this.catalog = catalog;
      this.identifier =
          String.join("\u0000", identifier.namespace()) + "\u0000" + identifier.name();
      this.operation = operation;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // cache keys intentionally use identity
    public boolean equals(Object other) {
      if (!(other instanceof TableLoadKey)) {
        return false;
      }
      TableLoadKey that = (TableLoadKey) other;
      return catalog == that.catalog
          && identifier.equals(that.identifier)
          && operation.equals(that.operation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(System.identityHashCode(catalog), identifier, operation);
    }
  }
}
