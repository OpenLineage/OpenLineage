/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static io.openlineage.spark.agent.util.PathUtils.GLUE_TABLE_PREFIX;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.spark.agent.util.AwsUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class IcebergHandler implements CatalogHandler {
  private static final String ICEBERG_PATH_IDENTIFIER_CLASS_NAME =
      "org.apache.iceberg.spark.PathIdentifier";

  private final OpenLineageContext context;

  private static final String TYPE = "type";
  private static final String CATALOG_IMPL = "catalog-impl";

  public IcebergHandler(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean hasClasses() {
    try {
      IcebergHandler.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog");
      return true;
    } catch (Exception e) {
      log.debug("The iceberg catalog is not present");
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return (tableCatalog instanceof SparkCatalog) || (tableCatalog instanceof SparkSessionCatalog);
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String catalogName = tableCatalog.name();
    Map<String, String> sparkRuntimeConfig = ScalaConversionUtils.fromMap(session.conf().getAll());
    Map<String, String> catalogConf = getCatalogProperties(sparkRuntimeConfig, catalogName);
    String catalogType = getCatalogType(catalogConf);
    String warehouseLocation = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);

    // Several things to be aware of:
    // 1. You can read iceberg data without using an Iceberg catalog
    // 2. You can't write iceberg data without using an Iceberg catalog (Spark crashes)
    // 3. Iceberg will configure a default catalog called "default_iceberg". This catalog (usually)
    // lacks the warehouse property.
    // 4. When you read the metadata.json path of an Iceberg dataset, the concrete type of the
    // Identifier interface is "org.apache.iceberg.spark.PathIdentifier"

    // A heuristic to check for:
    // Is the catalog name "default_iceberg"?
    // Is the warehouse property set?
    // Is the identifier of type "org.apache.iceberg.spark.PathIdentifier"?
    // If the answer to all 3 is "YES" then we cannot assume that we are reading from a catalog that
    // belongs to this Spark application
    boolean isDefaultIcebergCatalog = "default_iceberg".equals(catalogName);
    boolean lacksWarehouseProperty =
        warehouseLocation == null || warehouseLocation.trim().isEmpty();
    boolean isPathIdentifier =
        ICEBERG_PATH_IDENTIFIER_CLASS_NAME.equals(identifier.getClass().getName());
    Optional<Table> table = getIcebergTable(tableCatalog, identifier);
    Optional<Path> maybeTableLocation = table.map(tbl -> new Path(tbl.location()));
    Optional<DatasetIdentifier> maybeSymlink = Optional.empty();
    if (isDefaultIcebergCatalog && lacksWarehouseProperty && isPathIdentifier) {
      if (log.isDebugEnabled()) {
        log.debug(
            "Encountered an Iceberg-formatted dataset ({}) that does not belong to the configured Iceberg catalog (catalog={})",
            identifierToString(identifier),
            catalogName);
      }
      maybeTableLocation = table.map(tbl -> new Path(tbl.location()));
    } else {
      if (log.isDebugEnabled()) {
        log.debug(
            "Encountered an Iceberg-formatted dataset ({}) that belongs to the configured Iceberg catalog (catalog={})",
            identifierToString(identifier),
            catalogName);
      }
      String tableName = identifier.toString();
      maybeSymlink = getSymlinkIdentifier(session, catalogType, catalogConf, tableName);
    }

    Path tableLocation =
        maybeTableLocation.orElseGet(
            () -> reconstructDefaultLocation(new Path(warehouseLocation), identifier));
    DatasetIdentifier di = PathUtils.fromPath(tableLocation);
    maybeSymlink.ifPresent(
        symlink -> di.withSymlink(symlink.getName(), symlink.getNamespace(), SymlinkType.TABLE));
    return di;
  }

  private String identifierToString(Identifier identifier) {
    Class<? extends Identifier> cls = identifier.getClass();
    String[] namespace = identifier.namespace();
    String ns = namespace.length > 1 ? Arrays.toString(namespace) : namespace[0];
    return String.format("%s(namespace=%s; name=%s)", cls.getSimpleName(), ns, identifier.name());
  }

  private Optional<DatasetIdentifier> getSymlinkIdentifier(
      SparkSession session, String catalogType, Map<String, String> catalogConf, String tableName) {
    String catalogUri = catalogConf.get(CatalogProperties.URI);
    DatasetIdentifier value;
    if ("hive".equals(catalogType)) {
      log.debug("Getting symlink for hive");
      value = getHiveIdentifier(session, catalogUri, tableName);
    } else if ("rest".equals(catalogType)) {
      log.debug("Getting symlink for rest");
      value = getRestIdentifier(catalogUri, tableName);
    } else if ("nessie".equals(catalogType)) {
      log.debug("Getting symlink for nessie");
      value = getNessieIdentifier(catalogUri, tableName);
    } else if ("glue".equals(catalogType)) {
      log.debug("Getting symlink for glue");
      value = getGlueIdentifier(tableName, session);
    } else {
      log.debug("Getting symlink using warehouse location and table name");
      String warehouseLocation = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);
      value =
          FilesystemDatasetUtils.fromLocationAndName(
              new Path(warehouseLocation).toUri(), tableName);
    }
    return Optional.ofNullable(value);
  }

  private void logMap(String message, Map<String, String> map) {
    if (log.isTraceEnabled()) {
      List<String> items = new ArrayList<>();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        items.add(entry.getKey() + ": " + entry.getValue());
      }
      items.sort(Comparator.naturalOrder());
      StringJoiner sj = new StringJoiner("\n\t", "\t", "");
      items.forEach(sj::add);
      log.trace("{}\n{}", message, sj);
    }
  }

  private Map<String, String> getCatalogProperties(Map<String, String> conf, String catalogName) {
    String propertyPrefix = String.format("spark.sql.catalog.%s.", catalogName);
    log.debug(
        "Searching for spark properties pertaining to the catalog '{}'. The catalog settings are prefixed with '{}'.",
        catalogName,
        propertyPrefix);
    logMap("The spark properties are:", conf);
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(propertyPrefix)) {
        String trimmedKey = key.substring(propertyPrefix.length());
        result.put(trimmedKey, entry.getValue());
      }
    }
    logMap("That catalog properties are:", result);
    return result;
  }

  @SneakyThrows
  private DatasetIdentifier getHiveIdentifier(
      SparkSession session, @Nullable String confUri, String table) {
    URI metastoreUri;
    if (confUri == null) {
      metastoreUri =
          SparkConfUtils.getMetastoreUri(session.sparkContext())
              .orElseThrow(() -> new UnsupportedCatalogException("hive"));
    } else {
      metastoreUri = new URI(confUri);
    }

    return new DatasetIdentifier(table, PathUtils.prepareHiveUri(metastoreUri).toString());
  }

  @SneakyThrows
  private DatasetIdentifier getNessieIdentifier(@Nullable String confUri, String table) {
    String uri = new URI(confUri).toString();
    return new DatasetIdentifier(table, uri);
  }

  @SneakyThrows
  private DatasetIdentifier getGlueIdentifier(String table, SparkSession sparkSession) {
    SparkContext sparkContext = sparkSession.sparkContext();
    String arn =
        AwsUtils.getGlueArn(sparkContext.getConf(), sparkContext.hadoopConfiguration()).get();
    return new DatasetIdentifier(GLUE_TABLE_PREFIX + table.replace(".", "/"), arn);
  }

  @SneakyThrows
  private DatasetIdentifier getRestIdentifier(@Nullable String confUri, String table) {
    String uri = new URI(confUri).toString();
    return new DatasetIdentifier(table, uri);
  }

  @Override
  public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
      Map<String, String> properties) {
    String format = properties.getOrDefault("format", "");
    return Optional.of(
        context.getOpenLineage().newStorageDatasetFacet("iceberg", format.replace("iceberg/", "")));
  }

  @SneakyThrows
  @Override
  public Optional<String> getDatasetVersion(
      TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
    return getIcebergTable(tableCatalog, identifier)
        .map(table -> table.currentSnapshot())
        .map(snapshot -> Long.toString(snapshot.snapshotId()));
  }

  @SneakyThrows
  private Optional<Table> getIcebergTable(TableCatalog tableCatalog, Identifier identifier) {
    try {
      if (tableCatalog instanceof SparkCatalog) {
        SparkCatalog sparkCatalog = (SparkCatalog) tableCatalog;
        SparkTable sparkTable = (SparkTable) sparkCatalog.loadTable(identifier);
        return Optional.ofNullable(sparkTable.table());
      } else {
        TableIdentifier tableIdentifier = TableIdentifier.parse(identifier.toString());
        SparkSessionCatalog sparkCatalog = (SparkSessionCatalog) tableCatalog;
        return Optional.ofNullable(sparkCatalog.icebergCatalog().loadTable(tableIdentifier));
      }
    } catch (NoSuchTableException e) {
      // don't log stack trace for missing tables
      log.warn("Failed to load table from catalog: {}", identifier);
      return Optional.empty();
    } catch (ClassCastException e) {
      log.error("Failed to load table from catalog: {}", identifier, e);
      return Optional.empty();
    }
  }

  private Path reconstructDefaultLocation(Path warehouseLocation, Identifier identifier) {
    // namespace1.namespace2.table -> /warehouseLocation/namespace1/namespace2/table
    String[] namespace = identifier.namespace();
    ArrayList<String> pathComponents = new ArrayList<>(namespace.length + 1);
    pathComponents.addAll(Arrays.asList(namespace));
    pathComponents.add(identifier.name());
    return new Path(warehouseLocation, String.join(Path.SEPARATOR, pathComponents));
  }

  @Override
  public String getName() {
    return "iceberg";
  }

  private String getCatalogType(Map<String, String> catalogConf) {
    if (catalogConf.containsKey(TYPE)) {
      String catalogType = catalogConf.get(TYPE);
      log.debug(
          "Found the catalog type using the 'type' property. The catalog type is '{}'",
          catalogType);
      return catalogType;
    } else if (catalogConf.containsKey(CATALOG_IMPL)
        && catalogConf.get(CATALOG_IMPL).endsWith("GlueCatalog")) {
      log.debug(
          "Default the catalog type to 'glue' because the catalog impl is {}",
          catalogConf.get(CATALOG_IMPL));
      return "glue";
    } else {
      return null;
    }
  }
}
