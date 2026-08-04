/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.jspecify.annotations.NonNull;

@Slf4j
public class IcebergHandler implements CatalogHandler {

  private final OpenLineageContext context;
  private final List<BaseCatalogTypeHandler> catalogTypeHandlers;

  static final String TYPE = "type";
  static final String CATALOG_IMPL = "catalog-impl";

  public IcebergHandler(OpenLineageContext context) {
    this.context = context;
    this.catalogTypeHandlers =
        Arrays.asList(
            // S3TablesCatalogTypeHandler must run first: its warehouse-ARN and glue.id signals
            // must override the suffix-based matches in GlueCatalogTypeHandler and the type=rest
            // match in RestCatalogTypeHandler when the underlying catalog is S3 Tables.
            new S3TablesCatalogTypeHandler(),
            new NessieCatalogTypeHandler(),
            new GlueCatalogTypeHandler(),
            new SnowflakeCatalogTypeHandler(),
            //            new LakehouseRestCatalogTypeHandler(),
            new RestCatalogTypeHandler(),
            new BigQueryMetastoreCatalogTypeHandler(),
            new JdbcCatalogTypeHandler(),
            new HadoopCatalogTypeHandler(),
            new HiveCatalogTypeHandler());
  }

  @Override
  public boolean hasClasses() {
    try {
      IcebergHandler.class.getClassLoader().loadClass("org.apache.iceberg.spark.SparkCatalog");
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      log.debug("The iceberg spark catalog is not present");
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
    String catalogType = catalogTypeHandler.getFacetType(conf);

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

    Map<String, String> catalogConf = getCatalogConf(session, tableCatalog);
    BaseCatalogTypeHandler catalogTypeHandler =
        getCatalogTypeHandler(getCatalogConf(session, tableCatalog));
    DatasetIdentifier primaryIdentifier =
        catalogTypeHandler.getPrimaryIdentifier(session, catalogConf, identifier, tableCatalog);

    return catalogTypeHandler
        .getSymlinkIdentifiers(session, catalogConf, identifier.toString())
        .map(primaryIdentifier::withSymlink)
        .orElse(primaryIdentifier);
  }

  private @NonNull Map<String, String> getCatalogConf(
      SparkSession session, TableCatalog tableCatalog) {
    String catalogName = tableCatalog.name();
    Map<String, String> sparkRuntimeConfig = ScalaConversionUtils.fromMap(session.conf().getAll());
    return getCatalogProperties(sparkRuntimeConfig, catalogName);
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
    return getCatalogTypeHandler(getCatalogProperties(properties, tableCatalog.name()))
        .getIcebergTable(tableCatalog, identifier)
        .map(Table::currentSnapshot)
        .map(snapshot -> Long.toString(snapshot.snapshotId()));
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
