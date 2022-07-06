/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class IcebergHandler implements CatalogHandler {

  private static final String TYPE = "type";

  @Override
  public boolean hasClasses() {
    try {
      IcebergHandler.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
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

    String prefix = String.format("spark.sql.catalog.%s", catalogName);
    Map<String, String> conf =
        ScalaConversionUtils.<String, String>fromMap(session.conf().getAll());
    log.info(conf.toString());
    Map<String, String> catalogConf =
        conf.entrySet().stream()
            .filter(x -> x.getKey().startsWith(prefix))
            .filter(x -> x.getKey().length() > prefix.length())
            .collect(
                Collectors.toMap(
                    x -> x.getKey().substring(prefix.length() + 1), // handle dot after prefix
                    Map.Entry::getValue));

    log.info(catalogConf.toString());
    if (catalogConf.isEmpty() || !catalogConf.containsKey(TYPE)) {
      throw new UnsupportedCatalogException(catalogName);
    }
    log.info(catalogConf.get(TYPE));
    switch (catalogConf.get(TYPE)) {
      case "hadoop":
        return getHadoopIdentifier(catalogConf, identifier.toString());
      case "hive":
        return getHiveIdentifier(
            session, catalogConf.get(CatalogProperties.URI), identifier.toString());
      default:
        throw new UnsupportedCatalogException(catalogConf.get(TYPE));
    }
  }

  private DatasetIdentifier getHadoopIdentifier(Map<String, String> conf, String table) {
    String warehouse = conf.get(CatalogProperties.WAREHOUSE_LOCATION);
    return PathUtils.fromPath(new Path(warehouse, table));
  }

  @SneakyThrows
  private DatasetIdentifier getHiveIdentifier(
      SparkSession session, @Nullable String confUri, String table) {
    String slashPrefixedTable = String.format("/%s", table);
    URI uri;
    if (confUri == null) {
      uri =
          SparkConfUtils.getMetastoreUri(session.sparkContext().conf())
              .orElseThrow(() -> new UnsupportedCatalogException("hive"));
    } else {
      uri = new URI(confUri);
    }
    return PathUtils.fromPath(
        new Path(PathUtils.enrichHiveMetastoreURIWithTableName(uri, slashPrefixedTable)));
  }

  @Override
  public Optional<TableProviderFacet> getTableProviderFacet(Map<String, String> properties) {
    String format = properties.getOrDefault("format", "");
    return Optional.of(new TableProviderFacet("iceberg", format.replace("iceberg/", "")));
  }

  @SneakyThrows
  @Override
  public Optional<String> getDatasetVersion(
      TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
    SparkCatalog sparkCatalog = (SparkCatalog) tableCatalog;
    SparkTable table;
    try {
      table = sparkCatalog.loadTable(identifier);
    } catch (NoSuchTableException ex) {
      return Optional.empty();
    }

    if (table.table() != null && table.table().currentSnapshot() != null) {
      return Optional.of(Long.toString(table.table().currentSnapshot().snapshotId()));
    }
    return Optional.empty();
  }

  @Override
  public String getName() {
    return "iceberg";
  }
}
