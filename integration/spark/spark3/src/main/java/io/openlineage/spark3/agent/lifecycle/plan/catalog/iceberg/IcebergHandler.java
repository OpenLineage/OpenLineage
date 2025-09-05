/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class IcebergHandler implements CatalogHandler {
  private static final String ICEBERG_PATH_IDENTIFIER_CLASS_NAME =
      "org.apache.iceberg.spark.PathIdentifier";

  private final OpenLineageContext context;
  private final List<BaseCatalogTypeHandler> catalogTypeHandlers;

  static final String TYPE = "type";
  static final String CATALOG_IMPL = "catalog-impl";

  public IcebergHandler(OpenLineageContext context) {
    this.context = context;
    this.catalogTypeHandlers =
        Arrays.asList(
            new NessieCatalogTypeHandler(),
            new GlueCatalogTypeHandler(),
            new RestCatalogTypeHandler(),
            new BigQueryMetastoreCatalogTypeHandler(),
            new HadoopCatalogTypeHandler(),
            new HiveCatalogTypeHandler());
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
  public Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
      TableCatalog tableCatalog, Map<String, String> properties) {
    Optional<Map<String, String>> catalogConf =
        context
            .getSparkSession()
            .map(SparkSession::conf)
            .map(conf -> conf.getAll())
            .map(ScalaConversionUtils::fromMap)
            .map(map -> getCatalogProperties(map, tableCatalog.name()));

    if (!catalogConf.isPresent()) {
      return Optional.empty();
    }
    Map<String, String> conf = catalogConf.get();
    BaseCatalogTypeHandler catalogTypeHandler = getCatalogTypeHandler(conf);
    String catalogType = Optional.ofNullable(conf.get(TYPE)).orElse(catalogTypeHandler.getType());

    OpenLineage.CatalogDatasetFacetBuilder builder =
        context
            .getOpenLineage()
            .newCatalogDatasetFacetBuilder()
            .name(tableCatalog.name())
            .framework("iceberg")
            .type(catalogType)
            .source("spark");

    String warehouseLocation = conf.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (warehouseLocation != null && !warehouseLocation.trim().isEmpty()) {
      builder.warehouseUri(warehouseLocation);
    }

    String catalogUri = conf.get(CatalogProperties.URI);
    if (catalogUri != null && !catalogUri.trim().isEmpty()) {
      builder.metadataUri(catalogUri);
    }

    Map<String, String> catalogProperties = catalogTypeHandler.catalogProperties(conf);
    if (!catalogProperties.isEmpty()) {
      OpenLineage.CatalogDatasetFacetCatalogPropertiesBuilder catalogPropertiesBuilder =
          context.getOpenLineage().newCatalogDatasetFacetCatalogPropertiesBuilder();
      catalogProperties.forEach(catalogPropertiesBuilder::put);
      builder.catalogProperties(catalogPropertiesBuilder.build());
    }
    return Optional.of(CatalogWithAdditionalFacets.of(builder.build()));
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
    BaseCatalogTypeHandler catalogTypeHandler = getCatalogTypeHandler(catalogConf);
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
      maybeSymlink = Optional.of(catalogTypeHandler.getIdentifier(session, catalogConf, tableName));
    }

    if (!maybeTableLocation.isPresent() && warehouseLocation == null) {
      log.debug(
          "The catalog type is 'rest' and the table location and warehouse location is empty. This is likely a table that is being created");
      throw new MissingDatasetIdentifierCatalogException(
          "No table location found. Probably needs to create table first");
    }

    Path tableLocation =
        maybeTableLocation.orElseGet(
            () -> catalogTypeHandler.defaultTableLocation(new Path(warehouseLocation), identifier));
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
    } catch (ClassCastException e) {
      log.error("Failed to load table from catalog: {}", identifier, e);
      return Optional.empty();
    } catch (Exception e) {
      if (e instanceof org.apache.spark.sql.catalyst.analysis.NoSuchTableException
          || e instanceof org.apache.iceberg.exceptions.NoSuchTableException) {
        // probably trying to obtain table details on START event while table does not exist
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public String getName() {
    return "iceberg";
  }

  private BaseCatalogTypeHandler getCatalogTypeHandler(Map<String, String> catalogConf) {
    Optional<BaseCatalogTypeHandler> handler =
        catalogTypeHandlers.stream().filter(h -> h.matchesCatalogType(catalogConf)).findFirst();

    if (handler.isPresent()) {
      log.debug("Found handler for catalog type: {}", handler.get().getClass());
      return handler.get();
    } else {
      // https://github.com/apache/iceberg/blob/apache-iceberg-1.9.1/core/src/main/java/org/apache/iceberg/CatalogUtil.java#L298
      return new HiveCatalogTypeHandler();
    }
  }
}
