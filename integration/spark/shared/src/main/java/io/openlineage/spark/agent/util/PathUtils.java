/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.io.File;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.internal.StaticSQLConf;

@Slf4j
@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
public class PathUtils {
  public static DatasetIdentifier fromPath(Path path) {
    return fromURI(path.toUri());
  }

  public static DatasetIdentifier fromURI(URI location) {
    return FilesystemDatasetUtils.fromLocation(location);
  }

  /**
   * Create DatasetIdentifier from CatalogTable, using storage's locationURI if it exists. In other
   * way, use defaultTablePath.
   */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, SparkSession sparkSession) {
    String tableName = nameFromTableIdentifier(catalogTable.identifier());

    DatasetIdentifier di;
    URI uri;
    if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
      uri = catalogTable.storage().locationUri().get();
      di = fromURI(uri);
    } else {
      uri = prepareUriFromDefaultTablePath(catalogTable, sparkSession);
      di = fromURI(uri);
    }

    SparkContext sparkContext = sparkSession.sparkContext();
    SparkConf sparkConf = sparkContext.getConf();
    Configuration hadoopConf = sparkContext.hadoopConfiguration();
    Optional<URI> metastoreUri = extractMetastoreUri(sparkContext);
    if (metastoreUri.isPresent()) {
      // dealing with Hive tables
      URI hiveUri = prepareHiveUri(metastoreUri.get());
      DatasetIdentifier symlink = FilesystemDatasetUtils.fromLocationAndName(hiveUri, tableName);
      return di.withSymlink(
          symlink.getName(), symlink.getNamespace(), DatasetIdentifier.SymlinkType.TABLE);
    } else if (catalogTable.provider().isDefined()
        && "hive".equals(catalogTable.provider().get())
        && "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            .equals(
                SparkConfUtils.findHadoopConfigKey(
                        hadoopConf, "hive.metastore.client.factory.class")
                    .orElse(""))) {
      String region = System.getenv().get("AWS_DEFAULT_REGION");
      if (region == null || region.isEmpty()) {
        region = System.getenv("AWS_REGION");
      }
      String accountId =
          SparkConfUtils.findSparkConfigKey(sparkConf, "spark.glue.accountId").orElse("");
      return di.withSymlink(
          tableName,
          String.format("aws:glue:%s:%s", region, accountId),
          DatasetIdentifier.SymlinkType.TABLE);
    } else {
      URI warehouseUri = new URI(StringUtils.substringBeforeLast(uri.toString(), File.separator));
      DatasetIdentifier symlink =
          FilesystemDatasetUtils.fromLocationAndName(warehouseUri, tableName);
      return di.withSymlink(
          symlink.getName(), symlink.getNamespace(), DatasetIdentifier.SymlinkType.TABLE);
    }
  }

  private static URI prepareUriFromDefaultTablePath(
      CatalogTable catalogTable, SparkSession sparkSession) {
    URI uri = sparkSession.sessionState().catalog().defaultTablePath(catalogTable.identifier());

    return uri;
  }

  @SneakyThrows
  public static URI prepareHiveUri(URI uri) {
    return new URI("hive", uri.getAuthority(), null, null, null);
  }

  private static Optional<URI> extractMetastoreUri(SparkContext context) {
    // make sure enableHiveSupport is called
    Optional<String> setting =
        SparkConfUtils.findSparkConfigKey(
            context.getConf(), StaticSQLConf.CATALOG_IMPLEMENTATION().key());
    if (!setting.isPresent() || !"hive".equals(setting.get())) {
      return Optional.empty();
    }

    return SparkConfUtils.getMetastoreUri(context);
  }

  private static String nameFromTableIdentifier(TableIdentifier identifier) {
    // we create name instead of calling `unquotedString` method which includes
    // spark_catalog
    // for Spark 3.4
    String name;
    if (identifier.database().isDefined()) {
      // include database in name
      name = String.format("%s.%s", identifier.database().get(), identifier.table());
    } else {
      // just table name
      name = identifier.table();
    }

    return name;
  }
}
