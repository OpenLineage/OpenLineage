/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import scala.Option;

@Slf4j
public class DeltaHandler implements CatalogHandler {
  private static final String DELTA = "delta";
  private static final String PATH_PROPERTY = "path";
  private final OpenLineageContext context;

  public DeltaHandler(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean hasClasses() {
    try {
      DeltaHandler.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.delta.catalog.DeltaCatalog");
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // If class does not exist or it's loading fails for some reason, we handle that failure by
      // returning false
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof DeltaCatalog;
  }

  @Override
  @SneakyThrows
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    boolean setActiveSession = !SparkSession.getActiveSession().isDefined();
    if (setActiveSession) {
      // Delta catalog loading resolves the global active session, which may be absent on listener
      // threads, most visibly while the final events are processed during application teardown.
      SparkSession.setActiveSession(session);
    }

    try {
      return getDatasetIdentifierFromCatalog(session, tableCatalog, identifier);
    } catch (Exception e) {
      if (!isMissingActiveSessionError(e)) {
        throw e;
      }
      // The session is no longer usable (e.g. SparkContext already stopped during application
      // teardown; Spark 4 rejects stopped sessions even when set as active), so the table cannot
      // be loaded from the Delta catalog anymore. Derive a best-effort location-based identifier
      // instead of dropping the dataset.
      log.warn(
          "Delta catalog lookup failed because no usable Spark session is available; "
              + "falling back to a location-based dataset identifier",
          e);
      return fallbackDatasetIdentifier(session, identifier, properties, e);
    } finally {
      if (setActiveSession) {
        SparkSession.clearActiveSession();
      }
    }
  }

  private static boolean isMissingActiveSessionError(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      String message = t.getMessage();
      if (message != null && message.contains("No active or default Spark session found")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds a dataset identifier without touching the Delta catalog, used when the catalog cannot be
   * queried anymore (no usable Spark session). Falls back to the table location from the plan
   * properties, or the default warehouse location; rethrows the original failure when neither is
   * available.
   */
  @SneakyThrows
  private DatasetIdentifier fallbackDatasetIdentifier(
      SparkSession session,
      Identifier identifier,
      Map<String, String> properties,
      Exception cause) {
    // Path identifier (e.g. delta.`/some/path`): the identifier name is the location itself.
    if (new Path(identifier.name()).isAbsolute()) {
      return PathUtils.fromPath(new Path(identifier.name()));
    }

    Optional<String> location = location(properties);
    if (location.isPresent()) {
      return PathUtils.fromTableIdentifier(
          toTableIdentifier(identifier), session.sparkContext(), new Path(location.get()).toUri());
    }

    Optional<URI> warehouseLocation =
        PathUtils.getWarehouseLocation(
            session.sparkContext().getConf(), session.sparkContext().hadoopConfiguration());
    if (warehouseLocation.isPresent()) {
      Path defaultLocation =
          PathUtils.reconstructDefaultLocation(
              warehouseLocation.get().toString(), identifier.namespace(), identifier.name());
      return PathUtils.fromTableIdentifier(
          toTableIdentifier(identifier), session.sparkContext(), defaultLocation.toUri());
    }

    throw cause;
  }

  private static Optional<String> location(Map<String, String> properties) {
    Optional<String> tableLocation = property(properties, TableCatalog.PROP_LOCATION);
    return tableLocation.isPresent() ? tableLocation : property(properties, PATH_PROPERTY);
  }

  private static Optional<String> property(Map<String, String> properties, String name) {
    return properties.entrySet().stream()
        .filter(entry -> name.equalsIgnoreCase(entry.getKey()))
        .map(Map.Entry::getValue)
        .filter(value -> value != null && !value.isEmpty())
        .findFirst();
  }

  private static TableIdentifier toTableIdentifier(Identifier identifier) {
    String[] namespace = identifier.namespace();
    String database = null;
    if (namespace.length == 1) {
      // {"database"}
      database = namespace[0];
    } else if (namespace.length > 1) {
      // {"spark_catalog", "database"}
      database = namespace[1];
    }
    return new TableIdentifier(identifier.name(), Option.apply(database));
  }

  private DatasetIdentifier getDatasetIdentifierFromCatalog(
      SparkSession session, TableCatalog tableCatalog, Identifier identifier) {
    DeltaCatalog catalog = (DeltaCatalog) tableCatalog;

    Table table = catalog.loadTable(identifier);
    if (catalog.isPathIdentifier(identifier)) {
      // no information in metastore, only path
      Path path = new Path(identifier.name());
      return PathUtils.fromPath(path);
    }
    Map<String, String> sparkRuntimeConfig = ScalaConversionUtils.fromMap(session.conf().getAll());

    if (table instanceof DeltaTableV2) {
      DeltaTableV2 deltaTable = (DeltaTableV2) table;
      // catalogTable is Option, but it is empty only for path identifier
      Option<CatalogTable> catalogTable = deltaTable.catalogTable();

      if (catalogTable.isDefined()) {
        return PathUtils.fromCatalogTable(catalogTable.get(), session);
      } else {
        return PathUtils.fromPath(deltaTable.path())
            .withSymlink(
                identifier.toString(),
                Optional.ofNullable(sparkRuntimeConfig.get("spark.sql.warehouse.dir"))
                    .orElse(tableCatalog.name()),
                SymlinkType.TABLE);
      }
    }

    // not a Delta table, fallback to SparkCatalog. See:
    // https://github.com/delta-io/delta/blob/v3.2.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaCatalog.scala#L193-L199
    V1Table v1Table = (V1Table) table;
    return PathUtils.fromCatalogTable(v1Table.catalogTable(), session);
  }

  @Override
  public Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      TableCatalog tableCatalog, Map<String, String> properties) {
    String name = tableCatalog.name();
    if (name == null || name.isEmpty()) {
      name = "spark_catalog"; // default
    }
    OpenLineage.CatalogDatasetFacetBuilder builder =
        context
            .getOpenLineage()
            .newCatalogDatasetFacetBuilder()
            .name(name)
            .framework(DELTA)
            .type(DELTA)
            .source("spark");

    return Optional.of(CatalogWithAdditionalFacets.of(builder.build()));
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    return Optional.of(
        context
            .getOpenLineage()
            .newStorageDatasetFacet(DELTA, "parquet")); // Delta is always parquet
  }

  @SneakyThrows
  @Override
  public Optional<String> getDatasetVersion(
      TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
    try {
      DeltaCatalog deltaCatalog = (DeltaCatalog) tableCatalog;
      return DeltaVersionUtils.getDatasetVersion(deltaCatalog.loadTable(identifier));
    } catch (Exception e) {
      if (isMissingActiveSessionError(e)) {
        log.warn(
            "Unable to resolve Delta dataset version without a usable Spark session; "
                + "omitting the version");
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public String getName() {
    return DELTA;
  }
}
