/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifierUtils;
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

  private static final String DEFAULT_SCHEME = "file";
  private static final String DEFAULT_SEPARATOR = "/";

  public static DatasetIdentifier fromPath(Path path) {
    return fromPath(path, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromPath(Path path, String defaultScheme) {
    return fromURI(path.toUri(), defaultScheme);
  }

  public static DatasetIdentifier fromURI(URI location) {
    return fromURI(location, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromURI(URI location, String defaultScheme) {
    return DatasetIdentifierUtils.fromURI(location, defaultScheme);
  }

  /**
   * Create DatasetIdentifier from CatalogTable, using storage's locationURI if it exists. In other
   * way, use defaultTablePath.
   */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, SparkSession sparkSession) {
    DatasetIdentifier di;
    URI uri;
    if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
      uri = prepareUriFromLocation(catalogTable);
      di = PathUtils.fromURI(uri, DEFAULT_SCHEME);
    } else {
      uri = prepareUriFromDefaultTablePath(catalogTable, sparkSession);
      di = PathUtils.fromURI(uri, DEFAULT_SCHEME);
    }

    SparkContext sparkContext = sparkSession.sparkContext();
    SparkConf sparkConf = sparkContext.getConf();
    Configuration hadoopConf = sparkContext.hadoopConfiguration();
    Optional<URI> metastoreUri = extractMetastoreUri(sparkContext);
    if (metastoreUri.isPresent()) {
      // dealing with Hive tables
      DatasetIdentifier symlink = prepareHiveDatasetIdentifier(catalogTable, metastoreUri.get());
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
          nameFromTableIdentifier(catalogTable.identifier()),
          String.format("aws:glue:%s:%s", region, accountId),
          DatasetIdentifier.SymlinkType.TABLE);
    } else {
      return di.withSymlink(
          nameFromTableIdentifier(catalogTable.identifier()),
          StringUtils.substringBeforeLast(uri.toString(), File.separator),
          DatasetIdentifier.SymlinkType.TABLE);
    }
  }

  private static URI prepareUriFromDefaultTablePath(
      CatalogTable catalogTable, SparkSession sparkSession) {
    URI uri = sparkSession.sessionState().catalog().defaultTablePath(catalogTable.identifier());

    return uri;
  }

  @SneakyThrows
  private static URI prepareUriFromLocation(CatalogTable catalogTable) {
    URI uri = catalogTable.storage().locationUri().get();

    if (uri.getPath() != null
        && uri.getPath().startsWith(DEFAULT_SEPARATOR)
        && uri.getScheme() == null) {
      uri = new URI(DEFAULT_SCHEME, null, uri.getPath(), null, null);
    } else if (uri.getScheme() != null && uri.getScheme().equals(DEFAULT_SCHEME)) {
      // Normalize the URI if it is already a file scheme but has three slashes
      String path = uri.getPath();
      if (uri.toString().startsWith(DEFAULT_SCHEME + ":///")) {
        uri = new URI(DEFAULT_SCHEME, null, path, null, null);
      }
    }

    return uri;
  }

  @SneakyThrows
  private static DatasetIdentifier prepareHiveDatasetIdentifier(
      CatalogTable catalogTable, URI metastoreUri) {
    String qualifiedName = nameFromTableIdentifier(catalogTable.identifier());
    if (!qualifiedName.startsWith(DEFAULT_SEPARATOR)) {
      qualifiedName = String.format("/%s", qualifiedName);
    }
    return PathUtils.fromPath(
        new Path(enrichHiveMetastoreURIWithTableName(metastoreUri, qualifiedName)));
  }

  @SneakyThrows
  public static URI enrichHiveMetastoreURIWithTableName(URI metastoreUri, String qualifiedName) {
    return new URI(
        "hive", null, metastoreUri.getHost(), metastoreUri.getPort(), qualifiedName, null, null);
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
