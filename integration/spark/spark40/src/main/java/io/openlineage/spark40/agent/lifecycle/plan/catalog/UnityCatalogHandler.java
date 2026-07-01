/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.DeltaVersionUtils;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;

@Slf4j
@RequiredArgsConstructor
public class UnityCatalogHandler implements CatalogHandler {

  /**
   * Canonical name of the open source Unity Catalog Spark catalog. Compared as a String so that the
   * {@code unitycatalog-spark} connector is not required on the compile classpath.
   */
  private static final String UC_SINGLE_CATALOG_CLASS_NAME =
      "io.unitycatalog.spark.UCSingleCatalog";

  private final OpenLineageContext context;

  @Override
  public boolean hasClasses() {
    try {
      UnityCatalogHandler.class.getClassLoader().loadClass(UC_SINGLE_CATALOG_CLASS_NAME);
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // If the class does not exist, or its loading fails for some reason, we handle that failure
      // by returning false
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return UC_SINGLE_CATALOG_CLASS_NAME.equals(tableCatalog.getClass().getCanonicalName());
  }

  /**
   * Namespace scheme for the Unity Catalog naming convention {@code unitycatalog://{host}[:port]}.
   */
  private static final String UNITY_CATALOG_NAMESPACE_SCHEME = "unitycatalog";

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {

    Table table;
    try {
      table = tableCatalog.loadTable(identifier);
    } catch (NoSuchTableException e) {
      log.error("Failed to get dataset identifier because table {} doesn't exist", identifier, e);
      throw new RuntimeException(e);
    }

    CatalogTable catalogTable;
    if (table instanceof DeltaTableV2) {
      // catalogTable is Option, but it is empty only for path identifier
      catalogTable = ((DeltaTableV2) table).catalogTable().get();
    } else if (table instanceof V1Table) {
      // not a Delta table, fallback to SparkCatalog. See:
      // https://github.com/delta-io/delta/blob/v3.2.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaCatalog.scala#L193-L199
      catalogTable = ((V1Table) table).catalogTable();
    } else {
      // Fail clearly (and logged). Lineage for this table is skipped.
      log.warn(
          "Unity Catalog table {} has unsupported type {}; expected a Delta or V1 table",
          identifier,
          table.getClass().getName());
      throw new UnsupportedCatalogException(
          "Unsupported Unity Catalog table type: " + table.getClass().getName());
    }

    // Physical storage location (e.g. abfss://..., s3://..., file:/...). FilesystemDatasetUtils
    // resolves the proper object-storage namespace (Azure ADLS Gen2 abfss/abfs included).
    URI locationUri = PathUtils.getLocationUri(catalogTable, session);
    DatasetIdentifier locationIdentifier = FilesystemDatasetUtils.fromLocation(locationUri);

    // Like the other catalog handlers, the physical location is the primary identifier (so lineage
    // from different engines touching the same storage converges on one dataset node) and the Unity
    // Catalog {@code {catalog}.{schema}.{table}} name is attached as a TABLE symlink.
    Optional<String> serverUri =
        ScalaConversionUtils.asJavaOptional(
            session.conf().getOption("spark.sql.catalog." + tableCatalog.name() + ".uri"));
    if (!serverUri.isPresent()) {
      // Spark instantiates UCSingleCatalog from the same conf, so a functioning Unity Catalog
      // without this key should not be possible.
      log.error(
          "No Unity Catalog server URI configured for catalog {}; returning the location-based "
              + "dataset identifier without a Unity Catalog symlink",
          tableCatalog.name());
      return locationIdentifier;
    }

    return locationIdentifier.withSymlink(
        unityCatalogName(tableCatalog.name(), identifier),
        unityCatalogNamespace(serverUri.get()),
        DatasetIdentifier.SymlinkType.TABLE);
  }

  /**
   * Builds the Unity Catalog namespace {@code unitycatalog://{host}[:{port}]} from the server URI.
   * The scheme (almost always {@code https}) and any path are intentionally dropped so the
   * namespace stays consistent with the other warehouse namespaces in the OpenLineage naming spec
   * and remains stable regardless of the transport protocol.
   */
  static String unityCatalogNamespace(String serverUri) {
    URI parsed = URI.create(serverUri);
    String authority = parsed.getAuthority() != null ? parsed.getAuthority() : parsed.getPath();
    return UNITY_CATALOG_NAMESPACE_SCHEME + "://" + authority;
  }

  /** Builds the {@code {catalog}.{schema}.{table}} name for a Unity Catalog table. */
  static String unityCatalogName(String catalogName, Identifier identifier) {
    String schema = String.join(".", identifier.namespace());
    return catalogName + "." + schema + "." + identifier.name();
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    if ("delta".equals(properties.get("provider"))) {
      // Delta is always parquet
      return Optional.of(context.getOpenLineage().newStorageDatasetFacet("delta", "parquet"));
    } else {
      // TODO handle other formats. Unity catalog supports other formats for external tables
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  public Optional<String> getDatasetVersion(
      TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
    Table table = tableCatalog.loadTable(identifier);
    if (table instanceof DeltaTableV2) {
      return DeltaVersionUtils.getDatasetVersion(table);
    } else {
      // Unity Catalog supports non-Delta (external) tables, but only Delta tables versions are
      // supported at the moment
      log.warn(
          "Unity Catalog table {} is not a Delta table (got {}); no dataset version reported",
          identifier,
          table.getClass().getName());
      return Optional.empty();
    }
  }

  @Override
  public String getName() {
    return "unity";
  }
}
